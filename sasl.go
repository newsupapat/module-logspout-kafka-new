package kafka

import (
	"strconv"

	"github.com/Shopify/sarama"
)

func loadOptions(config *sarama.Config, options map[string]string) error {
	if protocol, ok := options["security.protocol"]; ok && protocol == "SASL_PLAINTEXT" {
		return loadSASLOptions(config, options)
	}

	return nil
}

func loadSASLOptions(config *sarama.Config, options map[string]string) error {
	config.Metadata.Full = true
	config.Net.SASL.Enable = true
	config.Net.SASL.Handshake = true
	config.Net.SASL.User = options["sasl.user"]
	config.Net.SASL.Password = options["sasl.password"]

	if err := loadSASLHandshakeVersion(config, options, sarama.SASLHandshakeV1); err != nil {
		return errorf("SASL version configuration is invalid.")
	}

	switch options["sasl.mechanism"] {
	case sarama.SASLTypeSCRAMSHA256:
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &SCRAMClient{HashGeneratorFcn: SHA256} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	case sarama.SASLTypeSCRAMSHA512:
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &SCRAMClient{HashGeneratorFcn: SHA512} }
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	}

	return nil
}

func loadSASLHandshakeVersion(config *sarama.Config, options map[string]string, defaulVersion int16) error {
	var err error
	if version, ok := options["sasl.version"]; ok {
		err = parseSASLHandshakeVersion(config, version)
	} else {
		config.Net.SASL.Version = defaulVersion
	}

	return err
}

func parseSASLHandshakeVersion(config *sarama.Config, version string) error {
	value, err := strconv.Atoi(version)
	if err == nil {
		config.Net.SASL.Version = int16(value)
	}

	return err
}
