package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/go-redis/redis"
	"github.com/yinqiwen/gotoolkit/logger"
	"github.com/yinqiwen/gotoolkit/xjson"
)

var errSessionInvalid = errors.New("Session Invalid")

type sessionData struct {
	scookies    []*http.Cookie
	cookieFile  string
	redisClient *redis.Client
}

func (s *sessionData) Close() {
	if nil != s.redisClient {
		s.redisClient.Close()
	}
}

// func login(session *sessionData) {
// 	dest := "https://xueqiu.com/snowman/login"
// 	form := url.Values{}
// 	//req.PostForm.Add("areacode", "86")
// 	form.Add("password", gConf.Passwd)
// 	form.Add("captcha", "")
// 	form.Add("remember_me", "true")
// 	form.Add("username", gConf.User)
// 	req, err := http.NewRequest("POST", dest, strings.NewReader(form.Encode()))
// 	if nil != err {
// 		log.Fatal(err)
// 	}
// 	req.Header.Add("Accept", "*/*")
// 	req.Header.Add("Accept-Language", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6")
// 	req.Header.Add("Accept-Encoding", "gzip, deflate, br")
// 	req.Header.Add("Cache-Control", "no-cache")
// 	req.Header.Add("Connection", "keep-alive")
// 	req.Header.Add("Pragma", "no-cache")
// 	req.Header.Add("Referer", "https://xueqiu.com/")
// 	req.Header.Add("Origin", "https://xueqiu.com")
// 	req.Header.Add("User-Agent", gConf.UA)
// 	req.Header.Add("X-Requested-With", "XMLHttpRequest")
// 	req.Header.Add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")

// 	res, err := http.DefaultClient.Do(req)
// 	if nil != err {
// 		log.Fatal(err)
// 	}
// 	session.loginCookies = res.Cookies()
// 	buf := &bytes.Buffer{}
// 	for _, c := range session.scookies {
// 		buf.WriteString(c.String())
// 		buf.WriteString("\n")
// 	}
// 	ioutil.WriteFile(session.cookieFile, buf.Bytes(), 0755)
// 	var reader io.Reader
// 	reader, err = gzip.NewReader(res.Body)
// 	if nil != err {
// 		reader = res.Body
// 	}
// 	content, err := ioutil.ReadAll(reader)
// 	if nil != err {
// 		log.Fatal(err)
// 	}

// 	fmt.Printf(string(content))
// }

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
			line = strings.TrimSpace(line)
			buffer.Reset()
			if strings.HasPrefix(line, "#") {
				continue
			}
			ss := strings.Split(line, "=")
			cookie := &http.Cookie{Name: ss[0], Value: ss[1]}
			session.scookies = append(session.scookies, cookie)
		}
	}
	return nil
}

func testSession(session *sessionData) error {
	dest := "https://xueqiu.com/user/get_user_wealth.json"
	req, err := http.NewRequest("GET", dest, nil)
	if nil != err {
		log.Fatal(err)
	}
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Accept-Language", "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-TW;q=0.6")
	req.Header.Add("Accept-Encoding", "gzip, deflate, br")
	req.Header.Add("Cache-Control", "no-cache")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Pragma", "no-cache")
	req.Header.Add("Referer", "https://xueqiu.com/center/")
	req.Header.Add("User-Agent", gConf.UA)
	req.Header.Add("X-Requested-With", "XMLHttpRequest")
	for _, c := range session.scookies {
		req.AddCookie(c)
	}
	res, err := http.DefaultClient.Do(req)
	if nil != err {
		logger.Error("Failed to get user json:%v", err)
		return err
	}
	var reader io.Reader
	reader, err = gzip.NewReader(res.Body)
	if nil != err {
		reader = res.Body
	}
	content, err := ioutil.ReadAll(reader)
	if nil != err {
		logger.Error("Failed to read user json:%v", err)
		return err
	}
	v, err := xjson.Decode(bytes.NewBuffer(content))
	if nil != err {
		logger.Error("Invalid json %s with :%v", string(content), err)
		return err
	}
	if v.RGet("user_wallet").RGet("user_id").GetInt() != -1 {
		return nil
	}
	logger.Error("Invalid json res:%s", string(content))
	return errSessionInvalid
}

func initSession() (*sessionData, error) {
	session := &sessionData{}
	session.redisClient = redis.NewClient(&redis.Options{
		Addr:     gConf.RedisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	session.cookieFile = gConf.DataDir + "/Cookie"
	err := readCookie(session)
	if nil != err {
		logger.Error("No cookie found.")
		return nil, err
	}
	err = testSession(session)
	if nil != err {
		logger.Error("Maybe session cookie is invalid.")
		return nil, err
	}
	logger.Info("Session cookie is valid.")
	// if nil == err {
	// 	err = testSession(session)
	// 	if nil != err {
	// 		logger.Info("Try to login again since last cookie invalid.")
	// 	}
	// }
	// if nil != err {
	// 	login(session)
	// }
	return session, nil
}
