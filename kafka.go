package main

import (
	"errors"
	"io/ioutil"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	ClientCertFileName    = "client_cert.pem"
	ClientCertKeyFileName = "client_cert_key.pem"
	CACertFileName        = "ca_cert.pem"
	CertPath              = "/tmp/"
)

type Credentials struct {
	ClientCert    string `json:"client_cert"`
	ClientCertKey string `json:"client_cert_key"`
	CACert        string `json:"ca_cert"`
}

type Producer struct {
	*kafka.Producer
}

type Config struct {
	Brokers string       `json:"brokers"`
	Creds   *Credentials `json:"credentials"`
}

func NewProducer(brokers string, creds *Credentials) (*Producer, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": stripSchemaFromBrokerURL(brokers),
	}

	// Check to see if broker has SSL certs, if so write them out to disk and
	// set paths in librdkafka configMap
	if creds != nil {
		err := creds.writeCertsToFile()
		if err != nil {
			return nil, err
		}

		configMap.SetKey("ssl.key.location", creds.clientCertKeyFilePath())
		configMap.SetKey("ssl.certificate.location", creds.clientCertFilePath())
		configMap.SetKey("ssl.ca.location", creds.caCertFilePath())
		configMap.SetKey("security.protocol", "ssl")
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, err
	}

	return &Producer{p}, nil
}

func createProduceDestroy(topic string, brokers string, creds *Credentials) error {
	p, err := NewProducer(brokers, creds)
	if err != nil {
		return err
	}
	defer p.Close()

	value := strings.Repeat("this is a test.", 50)
	err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)}, nil)
	if err != nil {
		return err
	}

	return nil
}

func stripSchemaFromBrokerURL(s string) string {
	brokers := strings.Split(s, ",")

	for i, b := range brokers {
		brokers[i] = strings.Split(b, "://")[1]
	}

	return strings.Join(brokers, ",")
}

// writeCertsToFile writes the various SSL certificates to the file system and
// returns the path so that they may be used with librdkafka
func (c *Credentials) writeCertsToFile() error {

	// write ca cert
	err := ioutil.WriteFile(CertPath+CACertFileName, []byte(c.CACert), 0644)
	if err != nil {
		return errors.New("Unable to write CA Cert:" + err.Error())
	}

	err = ioutil.WriteFile(CertPath+ClientCertFileName, []byte(c.ClientCert), 0644)
	if err != nil {
		return errors.New("Unable to write Client Cert:" + err.Error())
	}

	err = ioutil.WriteFile(CertPath+ClientCertKeyFileName, []byte(c.ClientCertKey), 0644)
	if err != nil {
		return errors.New("Unable to write Client Cert Key:" + err.Error())
	}

	return nil
}

func (c *Credentials) clientCertKeyFilePath() string {
	return CertPath + ClientCertKeyFileName
}

func (c *Credentials) clientCertFilePath() string {
	return CertPath + ClientCertFileName
}

func (c *Credentials) caCertFilePath() string {
	return CertPath + CACertFileName
}
