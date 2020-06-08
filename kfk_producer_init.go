package kfktool

import (
	"io/ioutil"

	"github.com/Shopify/sarama"
	"gopkg.in/yaml.v2"
)

var (
	pConf    producerConf
	producer sarama.SyncProducer
)

// PInit ...
// @path: 配置文件路径
func PInit(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, &pConf)
	if err != nil {
		return err
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err = sarama.NewSyncProducer(pConf.Brokers, config)
	if err != nil {
		return err
	}
	return nil
}

// PClose ...
func PClose() {
	producer.Close()
}
