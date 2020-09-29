package kafka

import (
	"crypto/sha256"
	"crypto/sha512"

	"fmt"
	"reflect"
	"testing"
)

func Test_SCRAM_SHA256_client(t *testing.T) {
	client := &SCRAMClient{HashGeneratorFcn: SHA256}

	if h := client.HashGeneratorFcn(); reflect.TypeOf(h) != reflect.TypeOf(sha256.New()) {
		t.Error("Error to define hash generator function of SCRAM client")
	}

	if err := client.Begin("tutorial-producer", "tutorial-producer-password", ""); err != nil {
		t.Error("Error to begin prepares the client for the SCRAM exchange")
	}

	if _, err := client.Step(""); err != nil {
		t.Error("Error steps client through the SCRAM exchange")
	}

	if client.Done() {
		t.Error("SCRAM conversation's state is not correct")
	}
}

func Test_SCRAM_SHA512_client(t *testing.T) {
	client := &SCRAMClient{HashGeneratorFcn: SHA512}

	if h := client.HashGeneratorFcn(); reflect.TypeOf(h) != reflect.TypeOf(sha512.New()) {
		t.Error("Error to define hash generator function of SCRAM client")
	}

	if err := client.Begin("tutorial-producer", "tutorial-producer-password", ""); err != nil {
		t.Error("Error to begin prepares the client for the SCRAM exchange")
	}

	if _, err := client.Step(""); err != nil {
		t.Error("Error steps client through the SCRAM exchange")
	}

	if client.Done() {
		t.Error("SCRAM conversation's state is not correct")
	}
}

func Test_SCRAM_client_fail_invalid_parameters(t *testing.T) {
	invalidUser := fmt.Sprintf("tutorial-producer%#U", 0x0221)
	client := &SCRAMClient{HashGeneratorFcn: SHA512}
	if err := client.Begin(invalidUser, "tutorial-producer-password", ""); err == nil {
		t.Error("SCRAM client should return error when username contains any invalid character")
	}

	invalidPassword := fmt.Sprintf("tutorial-producer-password%#U", 0x024F)
	if err := client.Begin("tutorial-producer", invalidPassword, ""); err == nil {
		t.Error("SCRAM client should return error when password contains any invalid character")
	}

	invalidAuthzID := fmt.Sprintf("tutorial-authz-id%#U", 0x02FF)
	if err := client.Begin("tutorial-producer", "tutorial-producer-password", invalidAuthzID); err == nil {
		t.Error("SCRAM client should return error when authentication ID contains any invalid character")
	}
}
