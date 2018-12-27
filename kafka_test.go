package kafka

import (
	"os"
	"testing"
)

var noopts = map[string]string{}

func Test_read_route_address(t *testing.T) {
	address := "broker1:9092,broker2:9092"
	brokers := readBrokers(address)
	if len(brokers) != 2 {
		t.Fatal("expected two broker addrs")
	}
	if brokers[0] != "broker1:9092" {
		t.Errorf("broker1 addr should not be %s", brokers[0])
	}
	if brokers[1] != "broker2:9092" {
		t.Errorf("broker2 addr should not be %s", brokers[1])
	}
}

func Test_ignore_health_message(t *testing.T) {
	os.Unsetenv("KAFKA_IGNORE_MESSAGE_CONTAINS")
	if filterMessage("/hello/health") {
		t.Fatal("message should not be ignored")
	}

	os.Setenv("KAFKA_IGNORE_MESSAGE_CONTAINS", "/health,/test")
	if !filterMessage("/hello/health?random") {
		t.Fatal("message should be ignored")
	}
	if !filterMessage("/test?random") {
		t.Fatal("message should be ignored")
	}
	if filterMessage("/hello/info?random") {
		t.Fatal("message should not be ignored")
	}
	if filterMessage("") {
		t.Fatal("message should not be ignored")
	}
}

func Test_read_route_address_with_a_slash_topic(t *testing.T) {
	address := "broker/hello"
	brokers := readBrokers(address)
	if len(brokers) != 1 {
		t.Fatal("expected a broker addr")
	}

	topic := readTopic(address, noopts)
	if topic != "hello" {
		t.Errorf("topic should not be %s", topic)
	}
}

func Test_read_topic_option(t *testing.T) {
	opts := map[string]string{"topic": "hello"}
	topic := readTopic("", opts)
	if topic != "hello" {
		t.Errorf("topic should not be %s", topic)
	}
}

func Test_read_route_address_with_a_slash_topic_trumps_a_topic_option(t *testing.T) {
	opts := map[string]string{"topic": "trumped"}
	topic := readTopic("broker/hello", opts)
	if topic != "hello" {
		t.Errorf("topic should not be %s", topic)
	}
}
