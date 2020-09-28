package kafka

import (
	"os"
	"testing"

	"github.com/Shopify/sarama"
)

func Test_load_sasl_scram_sha256_options(t *testing.T) {
	os.Setenv("KAFKA_COMPRESSION_CODEC", "gzip")
	config := newConfig()
	options := map[string]string{
		"security.protocol": "SASL_PLAINTEXT",
		"sasl.mechanism":    sarama.SASLTypeSCRAMSHA256,
		"sasl.user":         "tutorial-producer",
		"sasl.password":     "tutorial-producer-password",
	}

	loadOptions(config, options)

	if !config.Net.SASL.Enable {
		t.Error("SASL option be not enabled")
	}

	if config.Net.SASL.User == "" {
		t.Error("SASL User option be not set")
	}

	if config.Net.SASL.Password == "" {
		t.Error("SASL Password option be not set")
	}

	if config.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA256 {
		t.Errorf("SASL Mechanism option be not %s", sarama.SASLTypeSCRAMSHA256)
	}

	if scramClient := config.Net.SASL.SCRAMClientGeneratorFunc(); scramClient == nil {
		t.Error("SASL's SCRAM client generator function be no work!")
	}
}

func Test_load_sasl_scram_sha512_options(t *testing.T) {
	os.Setenv("KAFKA_COMPRESSION_CODEC", "snappy")
	config := newConfig()
	options := map[string]string{
		"security.protocol": "SASL_PLAINTEXT",
		"sasl.version":      "1",
		"sasl.mechanism":    sarama.SASLTypeSCRAMSHA512,
		"sasl.user":         "tutorial-producer",
		"sasl.password":     "tutorial-producer-password",
	}

	loadOptions(config, options)

	if !config.Net.SASL.Enable {
		t.Error("SASL option be not enabled")
	}

	if config.Net.SASL.User == "" {
		t.Error("SASL User option be not set")
	}

	if config.Net.SASL.Password == "" {
		t.Error("SASL Password option be not set")
	}

	if config.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA512 {
		t.Errorf("SASL Mechanism option be not %s", sarama.SASLTypeSCRAMSHA512)
	}

	if scramClient := config.Net.SASL.SCRAMClientGeneratorFunc(); scramClient == nil {
		t.Error("SASL's SCRAM client generator function be no work!")
	}
}
