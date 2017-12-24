package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

var gConf SnotifyConfig

type sessionData struct {
	loginCookies []*http.Cookie
	cookieFile   string
}

func login(session *sessionData) {

	dest := "https://xueqiu.com/snowman/login"

	form := url.Values{}
	//req.PostForm.Add("areacode", "86")
	form.Add("password", gConf.Passwd)
	form.Add("captcha", "")
	form.Add("remember_me", "true")
	form.Add("username", gConf.User)
	req, err := http.NewRequest("POST", dest, strings.NewReader(form.Encode()))
	if nil != err {
		log.Fatal(err)
	}
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Accept-Language", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6")
	req.Header.Add("Accept-Encoding", "gzip, deflate, br")
	req.Header.Add("Cache-Control", "no-cache")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Pragma", "no-cache")
	req.Header.Add("Referer", "https://xueqiu.com/")
	req.Header.Add("Origin", "https://xueqiu.com")
	req.Header.Add("User-Agent", "Mozilla/5.0 (iPad; CPU OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1")
	req.Header.Add("X-Requested-With", "XMLHttpRequest")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")

	res, err := http.DefaultClient.Do(req)
	if nil != err {
		log.Fatal(err)
	}
	session.loginCookies = res.Cookies()
	buf := &bytes.Buffer{}
	for _, c := range session.loginCookies {
		buf.WriteString(c.String())
		buf.WriteString("\n")
	}
	ioutil.WriteFile(session.cookieFile, buf.Bytes(), 0755)
	var reader io.Reader
	reader, err = gzip.NewReader(res.Body)
	if nil != err {
		reader = res.Body
	}
	content, err := ioutil.ReadAll(reader)
	if nil != err {
		log.Fatal(err)
	}

	fmt.Printf(string(content))
}

func getPortfolio(session *sessionData, portfolio string) {
	dest := fmt.Sprintf("https://xueqiu.com/cubes/rebalancing/history.json?cube_symbol=%s&count=20&page=1", portfolio)
	req, err := http.NewRequest("GET", dest, nil)
	if nil != err {
		log.Fatal(err)
	}
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Accept-Encoding", "gzip, deflate, br")
	req.Header.Add("Cache-Control", "no-cache")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Pragma", "no-cache")
	req.Header.Add("Referer", "https://xueqiu.com/P/"+portfolio)
	req.Header.Add("User-Agent", "Mozilla/5.0 (iPad; CPU OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1")
	req.Header.Add("X-Requested-With", "XMLHttpRequest")
	for _, c := range session.loginCookies {
		req.AddCookie(c)
	}
	//req.Header.Add("Cookie", cookie)

	res, err := http.DefaultClient.Do(req)
	if nil != err {
		log.Fatal(err)
	}
	var reader io.Reader
	reader, err = gzip.NewReader(res.Body)
	if nil != err {
		reader = res.Body
	}
	content, err := ioutil.ReadAll(reader)
	if nil != err {
		log.Fatal(err)
	}
	str := string(content)
	rec := &RebalanceRecord{}
	json.NewDecoder(strings.NewReader(str)).Decode(rec)

	tmp, _ := json.MarshalIndent(rec, "", "\t")
	log.Printf("%s", string(tmp))

}

func readCookie(session *sessionData) error {
	var f *os.File
	var err error
	if f, err = os.Open(session.cookieFile); err != nil {
		return err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	var buffer bytes.Buffer
	var (
		part   []byte
		prefix bool
	)
	reqbuf := &bytes.Buffer{}
	reqbuf.WriteString("HTTP/1.1 200 OK\r\n")

	for {
		if part, prefix, err = reader.ReadLine(); err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		buffer.Write(part)
		if !prefix {
			line := buffer.String()
			buffer.Reset()
			if strings.HasPrefix(line, "#") {
				continue
			}
			reqbuf.WriteString("Set-Cookie:")
			reqbuf.WriteString(line)
			reqbuf.WriteString("\r\n")
		}
	}
	reqbuf.WriteString("\r\n")
	res, err := http.ReadResponse(bufio.NewReader(reqbuf), nil)
	if nil != err {
		log.Printf("1####%s %v", reqbuf.String(), err)
		return err
	}
	//log.Printf("2###%v", res.Cookies())
	session.loginCookies = res.Cookies()
	return nil
}

func main() {
	conf := flag.String("conf", "", "Config file of snotify.")
	flag.Parse()
	confData, err := ioutil.ReadFile(*conf)
	if nil != err {
		log.Fatal(err)
	}
	err = json.Unmarshal(confData, &gConf)
	if nil != err {
		log.Fatal(err)
	}
	session := &sessionData{}
	os.Mkdir(gConf.DataDir, 0755)
	session.cookieFile = gConf.DataDir + "/Cookie"

	if nil != readCookie(session) {
		login(session)
	} else {
		log.Printf("Read cookie from local file.")
	}
	getPortfolio(session, "ZH003851")
}
