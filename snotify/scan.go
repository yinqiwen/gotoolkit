package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/yinqiwen/gotoolkit/logger"
)

type StockElement struct {
	Name   string
	Code   string
	Weight string
}

type PortfolioSummary struct {
	Name      string
	Code      string
	StockList []StockElement
}

type PortfolioActiveItem struct {
	Item    RebalanceItem
	Summary PortfolioSummary
}

func testParser(file string) {
	f, _ := os.Open(file)
	doc, err := goquery.NewDocumentFromReader(f)
	if nil != err {
		logger.Error("Invalid html with err:%v", err)
		return
	}
	vn := doc.Find(".cube-title .name").Text()
	fmt.Printf("%v\n", vn)
	doc.Find(".weight-list .stock").Each(func(i int, s *goquery.Selection) {
		// For each item found, get the band and title
		//ss, _ := goquery.OuterHtml(s)
		name := s.Find(".stock-name .name").Text()
		code := s.Find(".stock-name .price").Text()
		weight := s.Find(".stock-weight").Text()
		fmt.Printf("%v %v %v\n", name, code, weight)
		// band := s.Find("name").Text()
		// title := s.Find("i").Text()
		// fmt.Printf("Review %d: %s - %s\n", i, band, title)
	})
}

func getPortfolioSummary(session *sessionData, portfolio string) (*PortfolioSummary, bool, error) {
	changed := false
	dest := fmt.Sprintf("https://xueqiu.com/P/%s", portfolio)
	req, err := http.NewRequest("GET", dest, nil)
	if nil != err {
		return nil, changed, err
	}
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Accept-Encoding", "gzip, deflate, br")
	req.Header.Add("Cache-Control", "no-cache")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Pragma", "no-cache")
	req.Header.Add("User-Agent", gConf.UA)
	req.Header.Add("X-Requested-With", "XMLHttpRequest")
	for _, c := range session.loginCookies {
		req.AddCookie(c)
	}
	res, err := http.DefaultClient.Do(req)
	if nil != err {
		logger.Error("Failed to request summary:%v", err)
		return nil, changed, err
	}
	var reader io.Reader
	reader, err = gzip.NewReader(res.Body)
	if nil != err {
		reader = res.Body
	}
	doc, err := goquery.NewDocumentFromReader(reader)
	if nil != err {
		logger.Error("Invalid html with err:%v", err)
		return nil, changed, err
	}
	validHTML := true
	summary := &PortfolioSummary{}

	summary.Name = doc.Find(".cube-title .name").Text()
	if len(summary.Name) == 0 {
		validHTML = false
		logger.Error("No name found for summary info.")
	}
	if validHTML {
		doc.Find(".weight-list .stock").Each(func(i int, s *goquery.Selection) {
			stock := StockElement{}
			stock.Name = s.Find(".stock-name .name").Text()
			stock.Code = s.Find(".stock-name .price").Text()
			stock.Weight = s.Find(".stock-weight").Text()
			if len(stock.Name) > 0 && len(stock.Code) > 0 && len(stock.Weight) > 0 {
				summary.StockList = append(summary.StockList, stock)
			} else {
				logger.Error("No name/code/weight found for summary info.")
				validHTML = false
			}
		})
	}
	summary.Code = portfolio
	if validHTML {
		key := fmt.Sprintf("%s:summary", portfolio)
		overide := true
		content, _ := json.Marshal(summary)
		if val, err := session.redisClient.Get(key).Result(); nil == err {
			if string(content) == val {
				overide = false
			}
		}
		if overide {
			session.redisClient.Set(key, string(content), 0).Err()
			changed = true
		}
		return summary, changed, nil
	}

	return nil, changed, nil
}

func getPortfolio(session *sessionData, portfolio string) ([]PortfolioActiveItem, error) {
	summary, change, err := getPortfolioSummary(session, portfolio)
	if nil != err {
		logger.Error("failed to get summary:%v", err)
		return nil, err
	}
	if !change {
		return nil, nil
	}
	var active []PortfolioActiveItem
	dest := fmt.Sprintf("https://xueqiu.com/cubes/rebalancing/history.json?cube_symbol=%s&count=20&page=1", portfolio)
	req, err := http.NewRequest("GET", dest, nil)
	if nil != err {
		return active, err
	}
	req.Header.Add("Accept", "*/*")
	req.Header.Add("Accept-Encoding", "gzip, deflate, br")
	req.Header.Add("Cache-Control", "no-cache")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Pragma", "no-cache")
	req.Header.Add("Referer", "https://xueqiu.com/P/"+portfolio)
	req.Header.Add("User-Agent", gConf.UA)
	req.Header.Add("X-Requested-With", "XMLHttpRequest")
	for _, c := range session.loginCookies {
		req.AddCookie(c)
	}
	res, err := http.DefaultClient.Do(req)
	if nil != err {
		logger.Error("failed to get history:%v", err)
		return active, err
	}
	var reader io.Reader
	reader, err = gzip.NewReader(res.Body)
	if nil != err {
		reader = res.Body
	}
	content, err := ioutil.ReadAll(reader)
	if nil != err {
		logger.Error("failed to read history:%v", err)
		return active, err
	}
	str := string(content)
	rec := &RebalanceRecord{}
	err = json.NewDecoder(strings.NewReader(str)).Decode(rec)
	if nil != err {
		logger.Error("failed to read history json:%s :%v", str, err)
		return active, err
	}
	for _, item := range rec.Items {
		tm := time.Unix(item.CreatedAt/1000, 0)
		day := tm.Format("2006-01-02")
		logger.Info("%s have an rebalance record at %v", portfolio, tm)
		key := fmt.Sprintf("%s:%d", portfolio, item.Id)
		if _, err := session.redisClient.Get(key).Result(); nil != err {
			content, _ := json.Marshal(item)
			err = session.redisClient.Set(key, string(content), 0).Err()
			if nil != err {
				logger.Error("Redis err:%v", err)
			}
			session.redisClient.LPush(fmt.Sprintf("%s:his", portfolio), item.Id).Err()
			session.redisClient.SAdd(fmt.Sprintf("portfolio:%s", day), key).Err()
		} else {
			continue
		}
		activeItem := PortfolioActiveItem{}
		if nil != summary {
			activeItem.Summary = *summary
		}
		activeItem.Item = item

		active = append(active, activeItem)
		hkey := fmt.Sprintf("%s:%d:entry", portfolio, item.Id)
		for _, entry := range item.Entry {
			exist, err := session.redisClient.HExists(hkey, entry.StockSymbol).Result()
			if nil != err {
				logger.Error("Redis err:%v", err)
				continue
			}
			if !exist {
				content, _ := json.Marshal(entry)
				err = session.redisClient.HSet(hkey, entry.StockSymbol, string(content)).Err()
				if nil != err {
					logger.Error("Redis err:%v", err)
				}
				if entry.PrevWeight < entry.Weight {
					session.redisClient.HIncrBy(fmt.Sprintf("stock:%s", day), entry.StockSymbol+":buy", 1).Err()
				} else {
					session.redisClient.HIncrBy(fmt.Sprintf("stock:%s", day), entry.StockSymbol+":sell", 1).Err()
				}
			}
		}
	}
	return active, nil
	// tmp, _ := json.MarshalIndent(rec, "", "\t")
	// log.Printf("%s", string(tmp))

}

func scanPortfolio(session *sessionData, portfolio PortfolioConfig) ([]PortfolioActiveItem, error) {
	active, err := getPortfolio(session, portfolio.Name)
	if nil != err {
		logger.Error("Failed to scan portfolio:%v with err:%v", portfolio.Name, err)
		return nil, err
	}
	return active, nil
}
