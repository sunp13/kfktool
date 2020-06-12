package kfktool

import (
	"github.com/Shopify/sarama"
)

// ConfProducter ...
type ConfProducter []MyProducter

// MyProducter ...
type MyProducter struct {
	Alias         string              `yaml:"alias"`
	Brokers       []string            `yaml:"brokers"`
	Sync          bool                `yaml:"sync"`
	WaitAck       sarama.RequiredAcks `yaml:"wait_ack"`
	SuccLog       bool                `yaml:"succ_log"`
	Debug         bool                `yaml:"debug"`
	SyncProducter sarama.SyncProducer
	AsyncProducer sarama.AsyncProducer
	log           *Logger
}

// Dial ...
func (p *MyProducter) Dial() error {
	// p.Lock.Lock()
	// defer p.Lock.Unlock()
	p.Close()

	config := sarama.NewConfig()
	// 等待ACK的机制
	config.Producer.RequiredAcks = p.WaitAck
	config.Producer.Partitioner = sarama.NewHashPartitioner

	// 错误通道必须开启使用
	config.Producer.Return.Errors = true

	client, err := sarama.NewClient(p.Brokers, config)
	if err != nil {
		return err
	}

	if p.Sync {
		// 同步方式必须要有return.successes = true
		config.Producer.Return.Successes = true
		producer, err := sarama.NewSyncProducerFromClient(client)
		if err != nil {
			return err
		}
		p.log = NewLogger("", p.Alias, true, "only_console")
		p.log.debug = p.Debug
		p.SyncProducter = producer
	} else {
		// 异步的方式自己决定是否使用return.success
		config.Producer.Return.Successes = p.SuccLog

		producer, err := sarama.NewAsyncProducerFromClient(client)
		if err != nil {
			return err
		}
		p.AsyncProducer = producer

		// 默认日志接收器
		p.log = NewLogger("", p.Alias, true, "only_console")
		p.log.debug = p.Debug

		// 如果创建完成异步发送器后 需要对日志进行收集
		go func(mp *MyProducter) {
			for {
				select {
				case succMsg := <-p.AsyncProducer.Successes():
					p.log.PInfo("publish succ at %d partition %d offset text %s", succMsg.Partition, succMsg.Offset, succMsg.Value)
				case errMsg := <-p.AsyncProducer.Errors():
					p.log.PErr("publish failed at %d partition %d offset text %s reason %s", errMsg.Msg.Partition, errMsg.Msg.Offset, errMsg.Msg.Value, errMsg.Err.Error())
				}
			}
		}(p)
	}
	return nil
}

// Close ...
func (p *MyProducter) Close() {
	if p.SyncProducter != nil {
		p.SyncProducter.Close()
		p.SyncProducter = nil
	}
	if p.AsyncProducer != nil {
		p.AsyncProducer.Close()
		p.AsyncProducer = nil
	}
}

// Publish ... 发消息
func (p *MyProducter) Publish(topic, value string, key ...string) (partition int32, offset int64, err error) {
	if p.Sync {
		partition, offset, err = p.syncPublish(topic, value, key)
	} else {
		p.asyncPublish(topic, value, key)
	}
	return
}

func (p *MyProducter) syncPublish(topic, value string, key []string) (partition int32, offset int64, err error) {
	if len(key) > 0 {
		partition, offset, err = p.SyncProducter.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key[0]),
			Value: sarama.StringEncoder(value),
		})
	} else {
		partition, offset, err = p.SyncProducter.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(value),
		})
	}

	if err != nil {
		// 记错误日志
		p.log.PErr("publish failed at %d partition %d offset text %s reason %s", partition, offset, value, err.Error())
		return
	}

	// 如果要打成功日志
	if p.SuccLog {
		p.log.PInfo("publish succ at %d partition %d offset text %s", partition, offset, value)
	}
	return
}

func (p *MyProducter) asyncPublish(topic, value string, key []string) {
	if len(key) > 0 {
		p.AsyncProducer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key[0]),
			Value: sarama.StringEncoder(value),
		}
	} else {
		p.AsyncProducer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(value),
		}
	}
}

// SetLogger ... 使用自定的日志器
func (p *MyProducter) SetLogger(logger *Logger) {
	p.log = logger
}
