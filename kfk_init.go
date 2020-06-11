package kfktool

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

var (
	// P default producter
	P *MyProducter
	// PS producter map
	PS map[string]*MyProducter
)

// PInit ...
// @path: 配置文件路径
func PInit(path string) error {

	PS = make(map[string]*MyProducter)

	// 读取文件
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	var c ConfProducter
	err = yaml.Unmarshal(data, &c)
	if err != nil {
		return err
	}
	for _, v := range c {
		if err := v.Dial(); err != nil {
			return err
		}
		if v.Alias == "default" {
			P = &v
		} else {
			PS[v.Alias] = &v
		}
	}
	return nil
}
