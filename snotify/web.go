package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/yinqiwen/gotoolkit/logger"
)

func startHTTPServer() {
	if len(gConf.Listen) == 0 {
		return
	}
	http.HandleFunc("/save_cookie", func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if nil != err {
			logger.Error("Invalid save cookie body:%v", err)
			return
		}
		logger.Info("Save cookie info %s with %s", gConf.cookieFile, string(data))
		ioutil.WriteFile(gConf.cookieFile, data, os.ModePerm)
		fmt.Fprintf(w, "Saved")
	})

	http.HandleFunc("/top1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")

		// var tmp []*stockResultA
		// s1 := &stockResultA{}
		// s1.Code = "11111"
		// s1.Name = "Test"
		// s1.Portfolio = append(s1.Portfolio, portfolioWeight{"A", "1", "10%"})
		// s1.Portfolio = append(s1.Portfolio, portfolioWeight{"B", "2", "20%"})
		// s2 := &stockResultA{}
		// s2.Code = "22222"
		// s2.Name = "Test2"
		// s2.Portfolio = append(s2.Portfolio, portfolioWeight{"C", "3", "10%"})
		// s2.Portfolio = append(s2.Portfolio, portfolioWeight{"D", "4", "20%"})
		// tmp = append(tmp, s1)
		// tmp = append(tmp, s2)

		content := genPageForAnalyzeResult(topNFromPortfolio())
		w.Write([]byte(content))
	})
	http.HandleFunc("/top2", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hi")
	})

	err := http.ListenAndServe(gConf.Listen, nil)
	if nil != err {
		logger.Error("Failed to start HTTP server with err:%v", err)
	}
}
