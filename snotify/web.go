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

	http.HandleFunc("/hi", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hi")
	})

	err := http.ListenAndServe(gConf.Listen, nil)
	if nil != err {
		logger.Error("Failed to start HTTP server with err:%v", err)
	}
}
