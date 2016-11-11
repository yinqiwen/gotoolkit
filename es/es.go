package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type DataSourceSetting struct {
	Pattern string
	Indice  string
	Type    string
	CW1     int
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
	Indice IndiceSetting
	Type   TypeSetting
	CP     []DataSourceSetting
}

func main() {
	file := flag.String("file", "./test.json", "Specify input file")
	cfile := flag.String("config", "config.json", "Specify config file")
	es := flag.String("es", "http://127.0.0.1:9200", "Specify elastic search address")
	count := flag.Int64("count", -1, "Specify total process count.")
	flag.Parse()

	b, err := ioutil.ReadFile(*cfile)
	if nil != err {
		log.Printf("Failed to load config file:%s for reason:%v", *cfile, err)
		return
	}
	var cfg ESConfig
	err = json.Unmarshal(b, &cfg)
	if nil != err {
		log.Printf("Failed to parse config for reason:%v", err)
		return
	}

	typeIndices := make(map[string][]string)
	for _, cp := range cfg.CP {
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

	for _, indice := range cfg.Indice.Name {
		esIndex := make(map[string]interface{})
		esAnalyzer := make(map[string]interface{})
		my_synonym := make(map[string]interface{})
		my_synonym["type"] = "synonym"
		my_synonym["synonyms_path"] = "analysis/synonym.txt"
		es_filter := make(map[string]interface{})
		es_filter["my_synonym"] = my_synonym
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
		if cfg.Indice.Shards > 0 {
			settting["number_of_shards"] = cfg.Indice.Shards
		}

		b, _ = json.Marshal(settting)
		dest := *es + "/" + indice
		req, _ := http.NewRequest("PUT", dest, bytes.NewBuffer(b))
		res, err := http.DefaultClient.Do(req)
		if nil != err {
			log.Printf("###Failed to create index with errror:%v", err)
			return
		} else {
			resb, _ := ioutil.ReadAll(res.Body)
			log.Printf("###Update index for %v with %s  %s", indice, string(b), string(resb))
			res.Body.Close()
		}

	}
	for t, props := range cfg.Type.Mapping {
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
		app := make(map[string]interface{})
		app["properties"] = propsMapping
		all := make(map[string]interface{})
		all["enabled"] = false
		app["_all"] = all

		// esMapping := make(map[string]interface{})
		// esMapping["mappings"] = app

		indices := typeIndices[t]
		for _, indice := range indices {
			dest := *es + "/" + indice + "/" + t + "/_mapping"
			b, _ := json.Marshal(app)
			req, _ := http.NewRequest("POST", dest, bytes.NewBuffer(b))
			res, err := http.DefaultClient.Do(req)
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
	var processCount int64
	start := time.Now()
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
			if strings.HasPrefix(line, "#") {
				buffer.Reset()
				continue
			}
			doc := make(map[string]interface{})
			err = json.Unmarshal([]byte(line), &doc)
			if nil != err {
				log.Printf("Failed to unmarshal json:%s to config for reason:%v", line, err)
				buffer.Reset()
				continue
			}
			//log.Printf("####%v", doc)
			idstr := ""
			url, urlexist := doc["url"]
			if !urlexist {
				log.Printf("No 'url' in line:%v", line)
				buffer.Reset()
				continue
			}
			urlstr := url.(string)
			id, exist := doc["id"]
			if !exist {
				idstr = getMD5Hash(urlstr)
			} else {
				idstr = id.(string)
			}

			var cp DataSourceSetting
			for _, v := range cfg.CP {
				if strings.Contains(urlstr, v.Pattern) || v.Pattern == "*" {
					cp = v
					if doc["content"] == "" && cp.Type == "news" && cp.C2 == "article" {
						cp.Type = "multimedia"
						cp.C2 = "video"
					}
					doc["cw1"] = cp.CW1
					doc["c1"] = cp.C1
					doc["c2"] = cp.C2
					//doc["class"] = v.Class
					break
				}
			}
			if cp.Indice == "" {
				log.Printf("No match type found for:%v", doc["url"])
				buffer.Reset()
				continue
			}
			delete(doc, "id")
			b, _ := json.Marshal(doc)
			buffer.Reset()
			buffer.Write(b)
			dest := *es + "/" + cp.Indice + "/" + cp.Type + "/" + idstr
			res, err1 := http.Post(dest, "application/json", &buffer)
			if nil != err1 {
				log.Printf("Failed to es:%v", err1)
				return
			}
			if res.StatusCode >= 300 {
				resb, _ := ioutil.ReadAll(res.Body)
				log.Printf("##Recv error %d:%s", res.StatusCode, string(resb))
			} else {
				if nil != res.Body {
					io.Copy(ioutil.Discard, res.Body)
					res.Body.Close()
				}
			}
			//log.Printf("###res:%v", etype)

			processCount++
			buffer.Reset()
			if *count > 0 && processCount >= *count {
				break
			}
			if processCount%10000 == 0 {
				log.Printf("#####Cost %v to put %d records", time.Now().Sub(start), processCount)
			}
		}
	}
	log.Printf("#####Cost %v to put %d records into es.", time.Now().Sub(start), processCount)
}
