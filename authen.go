package kafka

import (
	"log"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/xdg/scram"
)

type SASLAuthentication struct {
	options map[string]string
}

func NewSASLAuthentication(options map[string]string) *SASLAuthentication {
	if _, found := options["sasl.version"]; !found {
		options["sasl.version"] = "1"
	}

	return &SASLAuthentication{options}
}

func (authen *SASLAuthentication) SetConfig(config *sarama.Config) error {
	if os.Getenv("DEBUG") != "" {
		log.Println("SASL PLAINTEXT is enabled")
	}

	config.Metadata.Full = true
	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = authen.options["sasl.user"]
	config.Net.SASL.Password = authen.options["sasl.password"]

	if version, err := strconv.Atoi(authen.options["sasl.version"]); err == nil {
		config.Net.SASL.Version = int16(version)
	} else {
		return errorf("SASL version configuration is invalid.")
	}

	switch authen.options["sasl.mechanism"] {
	case sarama.SASLTypeSCRAMSHA256:
		authen.setHash(config, sarama.SASLTypeSCRAMSHA256, SHA256)
	case sarama.SASLTypeSCRAMSHA512:
		authen.setHash(config, sarama.SASLTypeSCRAMSHA512, SHA512)
	}

	return nil
}

func (auth *SASLAuthentication) setHash(config *sarama.Config, mechanism sarama.SASLMechanism, generator scram.HashGeneratorFcn) {
	if os.Getenv("DEBUG") != "" {
		log.Printf("SASL Mechanism: %s\n", mechanism)
	}

	config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &SCRAMClient{HashGeneratorFcn: generator} }
	config.Net.SASL.Mechanism = mechanism
}
