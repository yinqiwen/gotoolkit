package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"html/template"
	"time"

	"github.com/yinqiwen/gotoolkit/logger"
	gomail "gopkg.in/gomail.v2"
)

var lastWarnMailTime time.Time

func warnNoCookie() {
	if !lastWarnMailTime.IsZero() && time.Now().Sub(lastWarnMailTime) < 30*time.Minute {
		return
	}
	d := gomail.NewDialer("smtp.gmail.com", 587, gConf.Mail.User, gConf.Mail.Passwd)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	m := gomail.NewMessage()
	m.SetHeader("From", gConf.Mail.User)
	m.SetHeader("To", gConf.Mail.Notify...)
	//m.SetAddressHeader("Cc", "dan@example.com", "Dan")
	m.SetHeader("Subject", "!Snotify无效Cookie!")
	m.SetBody("text/plain", "NULL")
	err := d.DialAndSend(m)
	if err != nil {
		logger.Error("Failed to send email:%v", err)
		return
	}
	lastWarnMailTime = time.Now()
}

func mailNotifyInfo(items []PortfolioActiveItem) error {
	content, err := genMail(items)
	if nil != err {
		return err
	}
	//ioutil.WriteFile("x.html", []byte(content), os.ModePerm)
	d := gomail.NewDialer("smtp.gmail.com", 587, gConf.Mail.User, gConf.Mail.Passwd)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	m := gomail.NewMessage()
	m.SetHeader("From", gConf.Mail.User)
	m.SetHeader("To", gConf.Mail.Notify...)
	//m.SetAddressHeader("Cc", "dan@example.com", "Dan")
	m.SetHeader("Subject", "最近调仓行为列表")
	m.SetBody("text/html", content)
	err = d.DialAndSend(m)
	if err != nil {
		logger.Error("Failed to send email:%v", err)
		return err
	}
	return nil
}

func getActionVal(entry RebalanceEntry) string {
	if entry.PrevWeight < entry.Weight {
		return "买进"
	}
	return "卖出"
}
func getTime(ts int64) string {
	tm := time.Unix(ts/1000, 0)
	return tm.Format("2006-01-02 15:04:05")
}

func adjustVal(entry RebalanceEntry) string {
	return fmt.Sprintf("%.2f%%->%.2f%%", entry.PrevWeight, entry.Weight)
}

func genMail(items []PortfolioActiveItem) (string, error) {
	t := template.New("mail.tpl.html")
	t = t.Funcs(template.FuncMap{"getAction": getActionVal, "adjust": adjustVal, "getTime": getTime})
	var err error
	t, err = t.ParseFiles("./mail.tpl.html")
	if err != nil {
		logger.Error("####1 %v", err)
		return "", err
	}
	buf := new(bytes.Buffer)
	if err = t.Execute(buf, items); err != nil {
		logger.Error("####2 %v", err)
		return "", err
	}
	return buf.String(), nil
}
