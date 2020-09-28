package kafka

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewKafkaAdapter, "kafka")
}

type KafkaAdapter struct {
	route    *router.Route
	brokers  []string
	topic    string
	producer sarama.AsyncProducer
}

func NewKafkaAdapter(route *router.Route) (router.LogAdapter, error) {
	brokers := readBrokers(route.Address)
	if len(brokers) == 0 {
		return nil, errorf("The Kafka broker host:port is missing. Did you specify it as a route address?")
	}

	topic := readTopic(route.Address, route.Options)
	if topic == "" {
		return nil, errorf("The Kafka topic is missing. Did you specify it as a route option?")
	}

	var err error
	if os.Getenv("DEBUG") != "" {
		log.Printf("Starting Kafka producer for address: %s, topic: %s.\n", brokers, topic)
	}

	var retries int
	retries, err = strconv.Atoi(os.Getenv("KAFKA_CONNECT_RETRIES"))
	if err != nil {
		retries = 3
	}
	var producer sarama.AsyncProducer
	for i := 0; i < retries; i++ {
		config := newConfig()
		if err = loadOptions(config, route.Options); err != nil {
			return nil, errorf("Route options is invalid. %v", err)
		}

		producer, err = sarama.NewAsyncProducer(brokers, config)
		if err != nil {
			if os.Getenv("DEBUG") != "" {
				log.Println("Couldn't create Kafka producer. Retrying...", err)
			}
			if i == retries-1 {
				return nil, errorf("Couldn't create Kafka producer. %v", err)
			}
		} else {
			time.Sleep(1 * time.Second)
		}
	}

	return &KafkaAdapter{
		route:    route,
		brokers:  brokers,
		topic:    topic,
		producer: producer,
	}, nil
}

func filterMessage(str string) bool {
	if messageFilters := os.Getenv("KAFKA_IGNORE_MESSAGE_CONTAINS"); messageFilters != "" {
		for _, messageFilter := range strings.Split(messageFilters, ",") {
			if messageFilter != "" && strings.Contains(str, messageFilter) {
				return true
			}
		}
	}
	return false
}

func (a *KafkaAdapter) Stream(logstream chan *router.Message) {
	defer a.producer.Close()
	for rm := range logstream {

		if filterMessage(rm.Data) {
			continue
		}

		message, err := a.formatMessage(rm)
		if err != nil {
			log.Println("kafka:", err)
			a.route.Close()
			break
		}
		if message != nil {
			a.producer.Input() <- message
		}
	}
}

func newConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = "logspout"
	config.Producer.Return.Errors = false
	config.Producer.Return.Successes = false
	config.Producer.Flush.Frequency = 1 * time.Second
	config.Producer.RequiredAcks = sarama.WaitForLocal

	if opt := os.Getenv("KAFKA_COMPRESSION_CODEC"); opt != "" {
		switch opt {
		case "gzip":
			config.Producer.Compression = sarama.CompressionGZIP
		case "snappy":
			config.Producer.Compression = sarama.CompressionSnappy
		}
	}

	return config
}

func (a *KafkaAdapter) formatMessage(m *router.Message) (*sarama.ProducerMessage, error) {

	dockerInfo := DockerInfo{
		Name:     m.Container.Name,
		ID:       m.Container.ID,
		Image:    m.Container.Config.Image,
		Hostname: m.Container.Config.Hostname,
	}

	if os.Getenv("DOCKER_LABELS") != "" {
		dockerInfo.Labels = make(map[string]string)
		for label, value := range m.Container.Config.Labels {
			dockerInfo.Labels[strings.Replace(label, ".", "_", -1)] = value
		}
	}

	// tags := GetContainerTags(m.Container, a)
	// fields := GetLogstashFields(m.Container, a)

	var js []byte
	var data map[string]interface{}
	var err error

	data = make(map[string]interface{})
	data["message"] = m.Data
	data["docker"] = dockerInfo
	data["stream"] = m.Source

	// Return the JSON encoding
	if js, err = json.Marshal(data); err != nil {
		// Log error message and continue parsing next line, if marshalling fails
		log.Println("logstash: could not marshal JSON:", err)
		return nil, nil
	}
	js = append(js, byte('\n'))

	encoder := sarama.StringEncoder(js)

	return &sarama.ProducerMessage{
		Topic: a.topic,
		Value: encoder,
	}, nil
}

func readBrokers(address string) []string {
	if strings.Contains(address, "/") {
		slash := strings.Index(address, "/")
		address = address[:slash]
	}

	return strings.Split(address, ",")
}

func readTopic(address string, options map[string]string) string {
	var topic string
	if !strings.Contains(address, "/") {
		topic = options["topic"]
	} else {
		slash := strings.Index(address, "/")
		topic = address[slash+1:]
	}

	return topic
}

func errorf(format string, a ...interface{}) (err error) {
	err = fmt.Errorf(format, a...)
	if os.Getenv("DEBUG") != "" {
		fmt.Println(err.Error())
	}
	return
}

type DockerInfo struct {
	Name     string            `json:"name"`
	ID       string            `json:"id"`
	Image    string            `json:"image"`
	Hostname string            `json:"hostname"`
	Labels   map[string]string `json:"labels"`
}

func loadOptions(config *sarama.Config, options map[string]string) error {
	if os.Getenv("DEBUG") != "" {
		log.Println("Load options")
	}

	if protocol, ok := options["security.protocol"]; ok && protocol == "SASL_PLAINTEXT" {
		return loadSASLOptions(config, options)
	}

	return nil
}
