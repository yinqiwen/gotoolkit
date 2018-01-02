package main

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/go-redis/redis"
	"github.com/yinqiwen/gotoolkit/logger"
)

type stockResultA struct {
	Name      string
	Code      string
	Portfolio []string
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
					r.Portfolio = append(r.Portfolio, portfolio.Name)
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

func topNFromStockActivity(last int) {
	// redisClient := redis.NewClient(&redis.Options{
	// 	Addr:     gConf.RedisAddr,
	// 	Password: "", // no password set
	// 	DB:       0,  // use default DB
	// })
	// now := time.Now()
	// buy := make(map[string]*stockResultA)
	// sell := make(map[string]*stockResultA)
	// for i := 0; i < last; i++ {
	// 	tm := now.Sub(time.Duration(i*24) * time.Hour)
	// 	day := tm.Format("2006-01-02")
	// 	key := fmt.Sprintf("stock:%s", day)
	// 	vmap, err := redisClient.HGetAll(key).Result()
	// 	if nil != err {
	// 		logger.Error("Failed to get value for hgetall %s with err:%v", key, err)
	// 		continue
	// 	}
	// 	if nil != vmap {
	// 		for k, v := range vmap {

	// 		}
	// 	}
	// }
}
