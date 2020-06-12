package kfktool

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// MyConsumer ...
type MyConsumer struct {
	brokers   []string
	topics    []string
	count     int
	groupName string
	log       *Logger
}

// NewConsumerGroup 创建一个新的消费者组
func NewConsumerGroup(brokers []string, topics []string, count int, groupName string) *MyConsumer {
	logger := NewLogger("", groupName, true, "only_console")
	logger.debug = true

	return &MyConsumer{
		brokers:   brokers,
		topics:    topics,
		count:     count,
		groupName: groupName,
		log:       logger,
	}
}

// Consumer 开始消费
func (c *MyConsumer) Consumer(fn func(msg *sarama.ConsumerMessage) error) {
	wg := &sync.WaitGroup{}
	config := cluster.NewConfig()
	// 返回错误信息
	config.Consumer.Return.Errors = true
	// 返回重新平衡通知信息
	config.Group.Return.Notifications = true
	// 第一次从较新的offset开始
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	// 提交位移时间间隔
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky

	for i := 0; i < c.count; i++ {
		wg.Add(1)
		go c.consum(config, i, fn)
	}

	wg.Wait()
}

func (c *MyConsumer) consum(conf *cluster.Config, index int, fn func(msg *sarama.ConsumerMessage) error) {
	consumer, err := cluster.NewConsumer(c.brokers, c.groupName, c.topics, conf)
	if err != nil {
		c.log.CErr("consumer group %s id %d err %s", c.groupName, index, err.Error())
		return
	}
	defer consumer.Close()

	// 写错误日志
	go func() {
		for err := range consumer.Errors() {
			c.log.CErr("consumer group %s id %d err %s", c.groupName, index, err.Error())
		}
	}()

	// 写ntf数据
	go func() {
		for ntf := range consumer.Notifications() {
			c.log.CInfo("consumer group %s id %d ntf %s", c.groupName, index, ntf.Type.String())
		}
	}()

	// 开始消费数据
	for msg := range consumer.Messages() {
		// 先提交位移.
		consumer.MarkOffset(msg, "")
		// 再给用户调用消息
		if err := fn(msg); err != nil {
			c.log.CErr("consumer group %s id %d handle topic=%s partition=%d offset=%d value=%s has error %s", c.groupName, index, msg.Topic, msg.Partition, msg.Offset, string(msg.Value), err.Error())
		}
		// 判断是否记录接收日志
		if c.log.LogConsumerSucc {
			c.log.CInfo("consumer group %s id %d msg topic=%s partition=%d offset=%d value=%s", c.groupName, index, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		}
	}
}

// SetLogger SetLogger
func (c *MyConsumer) SetLogger(logger *Logger) {
	c.log = logger
}
