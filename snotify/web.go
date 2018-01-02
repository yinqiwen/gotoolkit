package main

import (
	"fmt"
	"html"
	"net/http"

	"github.com/yinqiwen/gotoolkit/logger"
)

func startHTTPServer() {
	if len(gConf.Listen) == 0 {
		return
	}
	http.HandleFunc("/topn", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
	})

	http.HandleFunc("/hi", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hi")
	})

	err := http.ListenAndServe(gConf.Listen, nil)
	if nil != err {
		logger.Error("Failed to start HTTP server with err:%v", err)
	}
}
