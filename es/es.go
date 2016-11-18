package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type DataSourceSetting struct {
	Pattern string
	Indice  string
	Type    string
	CP      string
	C1      string //class1
	C2      string //class2
}

type IndiceSetting struct {
	Name   []string
	Shards int
}

type TypeSetting struct {
	Mapping map[string][]string
}

func getMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

type ESConfig struct {
	ES          []string
	SynonymPath string
	Indice      IndiceSetting
	Type        TypeSetting
	CP          []DataSourceSetting
}

var gcfg ESConfig
var startTime time.Time
var processCount int64
var limitCount int64
var concurrentCount int32
var maxConcurrent int32

var esClient = &http.Client{}

func postData(line string) (bool, error) {
	doc := make(map[string]interface{})
	err := json.Unmarshal([]byte(line), &doc)
	if nil != err {
		log.Printf("Failed to unmarshal json:%s to config for reason:%v", line, err)
		return false, err
	}
	idstr := ""
	url, urlexist := doc["url"]
	if !urlexist {
		return false, fmt.Errorf("No 'url' in line:%v", line)
	}
	urlstr := url.(string)
	id, exist := doc["id"]
	if !exist {
		idstr = getMD5Hash(urlstr)
	} else {
		idstr = id.(string)
	}

	var cp DataSourceSetting
	for _, v := range gcfg.CP {
		if strings.Contains(urlstr, v.Pattern) || v.Pattern == "*" {
			cp = v
			if doc["content"] == "" && cp.Type == "news" && cp.C2 == "article" {
				cp.Type = "multimedia"
				cp.C2 = "video"
			}
			if len(cp.CP) > 0 {
				doc["cp"] = cp.CP
			}
			doc["c1"] = cp.C1
			doc["c2"] = cp.C2
			break
		}
	}
	if cp.Indice == "" {
		return false, fmt.Errorf("No match type found for:%v", doc["url"])
	}
	delete(doc, "id")
	if date, exist := doc["date"]; exist {
		if date == "0000-00-00 00:00:00" {
			delete(doc, "date")
		}
	}
	b, _ := json.Marshal(doc)
	dest := gcfg.ES[int(processCount)%len(gcfg.ES)] + "/" + cp.Indice + "/" + cp.Type + "/" + idstr

	res, err1 := esClient.Post(dest, "application/json", bytes.NewBuffer(b))
	if nil != err1 {
		return false, err1
	}
	if res.StatusCode >= 300 {
		resb, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		log.Printf("##Recv error %d:%s", res.StatusCode, string(resb))
	} else {
		if nil != res.Body {
			io.Copy(ioutil.Discard, res.Body)
			res.Body.Close()
		}
	}
	atomic.AddInt64(&processCount, 1)
	if processCount%10000 == 0 {
		log.Printf("#####Cost %v to put %d records", time.Now().Sub(startTime), processCount)
	}
	return false, nil
}

func main() {
	file := flag.String("file", "./test.json", "Specify input file")
	cfile := flag.String("config", "config.json", "Specify config file")
	count := flag.Int64("count", -1, "Specify total process count.")
	concurrent := flag.Int("concurrent", 1, "Specify max concurrent worker.")
	replica := flag.Int("replica", 1, "Specify replica count.")
	flag.Parse()

	tr := &http.Transport{
		MaxIdleConnsPerHost: 2 * int(*concurrent),
	}
	esClient.Transport = tr
	limitCount = *count
	b, err := ioutil.ReadFile(*cfile)
	if nil != err {
		log.Printf("Failed to load config file:%s for reason:%v", *cfile, err)
		return
	}
	err = json.Unmarshal(b, &gcfg)
	if nil != err {
		log.Printf("Failed to parse config for reason:%v", err)
		return
	}
	typeIndices := make(map[string][]string)
	for _, cp := range gcfg.CP {
		indices := typeIndices[cp.Type]
		found := false
		for _, k := range indices {
			if k == cp.Indice {
				found = true
				break
			}
		}
		if !found {
			indices = append(indices, cp.Indice)
			typeIndices[cp.Type] = indices
		}
	}

	for _, indice := range gcfg.Indice.Name {
		esIndex := make(map[string]interface{})
		esAnalyzer := make(map[string]interface{})
		mySynonym := make(map[string]interface{})
		mySynonym["type"] = "synonym"
		mySynonym["synonyms_path"] = gcfg.SynonymPath
		es_filter := make(map[string]interface{})
		es_filter["my_synonym"] = mySynonym
		ik_syno := make(map[string]interface{})
		//ik_syno["type"] = "custom"
		ik_syno["tokenizer"] = "ik_max_word"
		ik_syno["filter"] = []string{"my_synonym"}
		esAnalyzer["ik_syno"] = ik_syno
		esIndex["filter"] = es_filter
		esIndex["analyzer"] = esAnalyzer
		tmp := make(map[string]interface{})
		tmp["analysis"] = esIndex
		settting := make(map[string]interface{})
		settting["index"] = tmp
		if gcfg.Indice.Shards > 0 {
			settting["number_of_shards"] = gcfg.Indice.Shards
		}

		b, _ = json.Marshal(settting)

		dest := gcfg.ES[0] + "/" + indice
		log.Printf("####%v ", dest)
		req, _ := http.NewRequest("PUT", dest, bytes.NewBuffer(b))
		res, err := esClient.Do(req)
		if nil != err {
			log.Printf("###Failed to create index with errror:%v", err)
			return
		} else {
			resb, _ := ioutil.ReadAll(res.Body)
			log.Printf("###Update index for %v with %s  %s", indice, string(b), string(resb))
			res.Body.Close()
		}

		indexSetting := make(map[string]interface{})
		tmp = make(map[string]interface{})
		tmp["number_of_replicas"] = 0
		tmp["refresh_interval"] = "-1"
		indexSetting["index"] = tmp
		b, _ = json.Marshal(indexSetting)
		dest = gcfg.ES[0] + "/" + indice + "/_settings"
		req, _ = http.NewRequest("PUT", dest, bytes.NewBuffer(b))
		res, err = esClient.Do(req)
		if nil != err {
			log.Printf("###Failed to update index setting with errror:%v", err)
			return
		} else {
			resb, _ := ioutil.ReadAll(res.Body)
			log.Printf("###Update index setting for %v with %s  %s", indice, string(b), string(resb))
			res.Body.Close()
		}
	}
	for t, props := range gcfg.Type.Mapping {
		propsMapping := make(map[string]interface{})
		for _, prop := range props {
			propMap := make(map[string]string)
			propMap["type"] = "text"
			propMap["analyzer"] = "ik_syno"
			propMap["search_analyzer"] = "ik_syno"
			propMap["include_in_all"] = "true"
			propsMapping[prop] = propMap
		}
		c1 := make(map[string]interface{})
		c1["type"] = "keyword"
		propsMapping["c1"] = c1
		c2 := make(map[string]interface{})
		c2["type"] = "keyword"
		propsMapping["c2"] = c2
		cp := make(map[string]interface{})
		cp["type"] = "keyword"
		propsMapping["cp"] = cp
		date := make(map[string]interface{})
		date["type"] = "date"
		date["format"] = "yyyy-MM-dd HH:mm:ss"
		propsMapping["date"] = date
		app := make(map[string]interface{})
		app["properties"] = propsMapping
		all := make(map[string]interface{})
		all["enabled"] = false
		app["_all"] = all

		// esMapping := make(map[string]interface{})
		// esMapping["mappings"] = app

		indices := typeIndices[t]
		for _, indice := range indices {
			dest := gcfg.ES[0] + "/" + indice + "/" + t + "/_mapping"
			b, _ := json.Marshal(app)
			req, _ := http.NewRequest("POST", dest, bytes.NewBuffer(b))
			res, err := esClient.Do(req)
			if nil != err {
				log.Printf("###Failed to update mapping with errror:%v", err)
				return
			} else {
				resb, _ := ioutil.ReadAll(res.Body)
				log.Printf("###Update mapping for %s/%v with %s  %s", indice, t, string(b), string(resb))
				res.Body.Close()
			}
		}
	}

	var f *os.File
	if f, err = os.Open(*file); err != nil {
		log.Printf("###Error:%v", err)
		return
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	var buffer bytes.Buffer
	var (
		part   []byte
		prefix bool
	)
	startTime = time.Now()
	var wg sync.WaitGroup
	taskChan := make(chan string, *concurrent)

	for i := 0; i < *concurrent; i++ {
		wg.Add(1)
		go func() {
			for line := range taskChan {
				if len(line) == 0 {
					break
				}
				exit, err := postData(line)
				if nil != err {
					log.Printf("Failed to post data for reason:%v", err)
				}
				if exit {
					break
				}
			}
			wg.Done()
		}()
	}
	lineCount := int64(0)
	for {
		if part, prefix, err = reader.ReadLine(); err != nil {
			if err != io.EOF {
				return
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
			taskChan <- line
			lineCount++
			if limitCount > 0 && lineCount >= limitCount {
				break
			}
		}
	}
	close(taskChan)
	wg.Wait()
	log.Printf("#####Cost %v to put %d records into es.", time.Now().Sub(startTime), processCount)

	if *replica > 0 {
		for _, indice := range gcfg.Indice.Name {
			indexSetting := make(map[string]interface{})
			tmp := make(map[string]interface{})
			tmp["number_of_replicas"] = *replica
			tmp["refresh_interval"] = "1s"
			indexSetting["index"] = tmp
			b, _ := json.Marshal(indexSetting)
			dest := gcfg.ES[0] + "/" + indice + "/_settings"
			req, _ := http.NewRequest("PUT", dest, bytes.NewBuffer(b))
			res, err := http.DefaultClient.Do(req)
			if nil != err {
				log.Printf("###Failed to enable index replica with errror:%v", err)
				return
			} else {
				resb, _ := ioutil.ReadAll(res.Body)
				log.Printf("###Update index setting for %v with %s  %s", indice, string(b), string(resb))
				res.Body.Close()
			}
			dest = gcfg.ES[0] + "/" + indice + "/_forcemerge?max_num_segments=5"
			req, _ = http.NewRequest("POST", dest, nil)
			res, err = http.DefaultClient.Do(req)
			if nil != err {
				log.Printf("###Failed to enable index replica with errror:%v", err)
				return
			} else {
				resb, _ := ioutil.ReadAll(res.Body)
				log.Printf("###Update index setting for %v with %s  %s", indice, string(b), string(resb))
				res.Body.Close()
			}

		}
	}

}
