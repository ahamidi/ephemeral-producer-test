package main

import (
	"log"

	"github.com/joeshaw/envdecode"
)

const testTopic = "messages"

type Env struct {
	Host          string `env:"HOST"`
	Port          string `env:"PORT,required"`
	Brokers       string `env:"KAFKA_URL,required"`
	ClientCert    string `env:"KAFKA_CLIENT_CERT"`
	ClientCertKey string `env:"KAFKA_CLIENT_CERT_KEY"`
	TrustedCert   string `env:"KAFKA_TRUSTED_CERT"`
}

type AppConfig struct {
	Brokers       []string
	ClientCert    string
	ClientCertKey string
}

var appConfig AppConfig

func main() {
	log.Println("Ephemeral Kafka Producer Test")

	// App Setup
	var env Env
	envdecode.MustDecode(&env)

	// Prep creds
	creds := &Credentials{
		ClientCert:    env.ClientCert,
		ClientCertKey: env.ClientCertKey,
		CACert:        env.TrustedCert,
	}

	for {
		err := createProduceDestroy(testTopic, env.Brokers, creds)
		if err != nil {
			log.Fatal(err)
		}
	}

}
