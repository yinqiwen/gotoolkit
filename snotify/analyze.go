package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/yinqiwen/gotoolkit/logger"
)

type portfolioWeight struct {
	Portfolio string
	Name      string
	Weight    string
}

type stockResultA struct {
	Name      string
	Code      string
	Portfolio []portfolioWeight
	count     int
}

type ByPortfolioCount []*stockResultA

func (a ByPortfolioCount) Len() int           { return len(a) }
func (a ByPortfolioCount) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByPortfolioCount) Less(i, j int) bool { return len(a[i].Portfolio) > len(a[j].Portfolio) }

func topNFromPortfolio() []*stockResultA {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     gConf.RedisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	result := make(map[string]*stockResultA)
	for _, portfolio := range gConf.Portfolio {
		key := fmt.Sprintf("%s:summary", portfolio.Name)
		if val, err := redisClient.Get(key).Result(); nil == err {
			summary := &PortfolioSummary{}
			err := json.Unmarshal([]byte(val), summary)
			if nil != err {
				logger.Error("Failed to unmarshal json:%s", val)
			} else {
				for _, stock := range summary.StockList {
					r, exist := result[stock.Code]
					if !exist {
						r = &stockResultA{}
						r.Code = stock.Code
						r.Name = stock.Name
						result[stock.Code] = r
					}
					p := portfolioWeight{
						Portfolio: summary.Code,
						Name:      summary.Name,
						Weight:    stock.Weight,
					}
					r.Portfolio = append(r.Portfolio, p)
				}
			}
		}
	}
	redisClient.Close()

	var sortResult []*stockResultA
	for _, v := range result {
		sortResult = append(sortResult, v)
	}
	sort.Sort(ByPortfolioCount(sortResult))

	cutIndex := len(sortResult)
	for i, r := range sortResult {
		if len(r.Portfolio) <= 1 {
			cutIndex = i
			break
		}
		if i >= 9 {
			cutIndex = i
			break
		}
	}
	return sortResult[0:cutIndex]
}

func genPageForAnalyzeResult(s []*stockResultA) string {
	if len(s) == 0 {
		return ""
	}
	t := template.New("top.tpl.html")
	t = t.Funcs(template.FuncMap{"getAction": getActionVal, "adjust": adjustVal, "getTime": getTime})
	var err error
	t, err = t.ParseFiles("./top.tpl.html")
	if err != nil {
		logger.Error("####1 %v", err)
		return ""
	}
	buf := new(bytes.Buffer)
	if err = t.Execute(buf, s); err != nil {
		logger.Error("####2 %v", err)
		return ""
	}
	return buf.String()
}

func topNFromStockActivity(last int) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     gConf.RedisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	now := time.Now()
	rmap := make([]map[string]*stockResultA, 2)
	rmap[0] = make(map[string]*stockResultA)
	rmap[1] = make(map[string]*stockResultA)
	for i := 0; i < last; i++ {
		tm := now.AddDate(0, 0, i*-1)
		day := tm.Format("2006-01-02")
		key := fmt.Sprintf("stock:%s", day)
		vmap, err := redisClient.HGetAll(key).Result()
		if nil != err {
			logger.Error("Failed to get value for hgetall %s with err:%v", key, err)
			continue
		}
		if nil != vmap {
			for k, v := range vmap {
				ss := strings.Split(k, ":")
				stock := ss[0]
				count, _ := strconv.Atoi(v)
				var tmp map[string]*stockResultA
				if ss[0] == "buy" {
					tmp = rmap[0]
				} else {
					tmp = rmap[1]
				}
				r, exist := tmp[stock]
				if !exist {
					r = &stockResultA{}
					tmp[stock] = r
					r.Code = stock
				}
				r.count += count
			}
		}
	}
}
