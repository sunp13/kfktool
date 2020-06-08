package kfktool

// KfkConf kafka配置文件
type producerConf struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}
