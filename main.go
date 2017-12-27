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
	ClientCert    string `env:"KAFKA_CLIENT_CERT,required"`
	ClientCertKey string `env:"KAFKA_CLIENT_CERT_KEY,required"`
	TrustedCert   string `env:"KAFKA_TRUSTED_CERT,required"`
	Concurrency   int    `env:"CONCURRENCY,default=10"`
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

	err := creds.writeCertsToFile()
	if err != nil {
		log.Fatal("Unable to write certs to file system:", err)
	}

	for i := 0; i < env.Concurrency; i++ {
		log.Printf("Starting worker %d", i)
		go func() {
			for {
				err := createProduceDestroy(testTopic, env.Brokers, creds)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	done := make(chan bool)
	<-done

}
