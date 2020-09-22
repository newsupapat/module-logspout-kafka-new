package kafka

import (
	"github.com/Shopify/sarama"
)

func loadOptions(config *sarama.Config, options map[string]string) {
	if protocol, ok := options["security.protocol"]; ok && protocol == "SASL_PLAINTEXT" {
		loadSASLOptions(config, options)
	}
}

func loadSASLOptions(config *sarama.Config, options map[string]string) {
	config.Metadata.Full = true
	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = options["sasl.user"]
	config.Net.SASL.Password = options["sasl.password"]

	switch options["sasl.mechanism"] {
	case sarama.SASLTypeSCRAMSHA256:
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &SCRAMClient{HashGeneratorFcn: SHA256} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case sarama.SASLTypeSCRAMSHA512:
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &SCRAMClient{HashGeneratorFcn: SHA512} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	}
}
