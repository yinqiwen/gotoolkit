package config

import (
	"os"
	"reflect"
	"time"
)

type Config interface {
	Init(conf string) error
}

type ConfigHolder interface {
	Get() Config
}

type configFile struct {
	file          string
	lastModTime   time.Time
	lastCheckTime time.Time
	checkPeriod   time.Duration
	configData    Config
}

func (c *configFile) Get() Config {
	return c.configData
}

var files = make(map[string]configFile)

func newConfigByInterface(cfg Config) Config {
	val := reflect.ValueOf(cfg)
	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	}
	return reflect.New(val.Type()).Interface().(Config)
}

func configLoopFunc() {
	for {
		now := time.Now()
		for _, cfg := range files {
			if now.Sub(cfg.lastCheckTime) < cfg.checkPeriod {
				continue
			}
			cfg.lastCheckTime = now
			st, err := os.Stat(cfg.file)
			if nil != err {
				continue
			}
			modTime := st.ModTime()
			if modTime.After(cfg.lastModTime) {
				newCfg := newConfigByInterface(cfg.configData)
				err = newCfg.Init(cfg.file)
				if nil == err {
					cfg.configData = newCfg
				}
				cfg.lastModTime = modTime
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func WatchConfigFile(file string, period time.Duration, cfg Config) {

}

func WatchConfigDir(dir string, period time.Duration) {

}
