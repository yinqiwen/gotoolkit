package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/yinqiwen/gotoolkit/logger"
)

var gConf SnotifyConfig

func main() {
	conf := flag.String("conf", "./snotify.json", "Config file of snotify.")
	flag.Parse()
	confData, err := ioutil.ReadFile(*conf)
	if nil != err {
		log.Fatal(err)
	}
	err = json.Unmarshal(confData, &gConf)
	if nil != err {
		log.Fatal(err)
	}
	logger.InitLogger(gConf.Log)
	os.Mkdir(gConf.DataDir, 0755)
	gConf.cookieFile = gConf.DataDir + "/Cookies"

	//smtpMail("", "", nil)

	if gConf.ScanPeriod <= 0 {
		gConf.ScanPeriod = 10
	}
	if gConf.ScanInterval <= 0 {
		gConf.ScanInterval = 1
	}
	go startHTTPServer()
	//testParser("./t.html")
	ticker := time.NewTicker(time.Duration(gConf.ScanPeriod) * time.Second)
	for range ticker.C {
		now := time.Now()
		if isNonTradeDayTime(now) {
			continue
		}
		logger.Info("Start scan portfolios.")
		session, err := initSession()
		if nil != err {
			log.Printf("Failed to init session with err:%v", err)
			continue
		}
		var active []PortfolioActiveItem
		for _, portfolio := range gConf.Portfolio {
			tmp, err := scanPortfolio(session, portfolio)
			if nil != err {
				log.Printf("Failed to scan portfolio:%s", portfolio.Name)
			} else {
				active = append(active, tmp...)
			}
			time.Sleep(time.Duration(gConf.ScanInterval) * time.Second)
		}
		if len(active) > 0 {
			mailNotifyInfo(active)
		}
		session.Close()
	}

}
