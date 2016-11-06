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
	Weight  int
	Class   string
}
type IndiceSetting struct {
	Mapping map[string][]string
}

func getMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

type ESConfig struct {
	Indice IndiceSetting
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

	for indice, props := range cfg.Indice.Mapping {
		propsMapping := make(map[string]interface{})
		for _, prop := range props {
			propMap := make(map[string]string)
			propMap["type"] = "string"
			propMap["analyzer"] = "ik_syno"
			propMap["search_analyzer"] = "ik_syno"
			propMap["include_in_all"] = "true"
			propsMapping[prop] = propMap
		}
		app := make(map[string]interface{})
		app["properties"] = propsMapping
		all := make(map[string]interface{})
		all["enabled"] = false
		app["_all"] = all

		// esMapping := make(map[string]interface{})
		// esMapping["mappings"] = app
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
		b, _ := json.Marshal(settting)
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
		dest = *es + "/" + indice + "/app/_mapping"
		b, _ = json.Marshal(app)
		req, _ = http.NewRequest("POST", dest, bytes.NewBuffer(b))
		res, err = http.DefaultClient.Do(req)
		if nil != err {
			log.Printf("###Failed to update mapping with errror:%v", err)
			return
		} else {
			resb, _ := ioutil.ReadAll(res.Body)
			log.Printf("###Update mapping for %v with %s  %s", indice, string(b), string(resb))
			res.Body.Close()
		}
	}

	// 	settings := make(map[string]indiceSetting)
	// 	settings["default_data"] = indiceSetting{"default_data", `{
	//     "mappings": {
	//         "app": {
	//             "_all": {
	//                 "enabled": false
	//             },
	//             "properties": {
	//                 "title": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	// 				"desc": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	// 				"content": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	// 				"tag": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	// 				"category": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	// 				"seller": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	// 				"director": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	// 				"actor": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	// 				"answer_content": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	// 				"best_answer_content": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 }
	//             }
	//         }
	//     }
	// }`}

	//create index setting
	// 	index := `{
	//     "settings": {
	//         "index": {
	//             "analysis": {
	//                 "analyzer": {
	//                     "ik_pinyin_analyzer": {
	//                         "type": "custom",
	//                         "tokenizer": "ik_max_word",
	//                         "filter": [
	//                             "word_delimiter",
	// 							"my_pinyin",
	// 							"lowercase"
	//                         ]
	//                     }
	//                 },
	//                 "filter": {
	//                     "my_pinyin": {
	//                         "type": "pinyin",
	//                         "first_letter": "prefix",
	//                         "padding_char": " "
	//                     }
	//                 }
	//             }
	//         }
	//     }
	// }`
	// 	http.Post(*es+"/yybdata/", "application/json", strings.NewReader(index))
	//create mapping setting
	// 	mapping := `{
	//     "mappings": {
	//         "app": {
	//             "_all": {
	//                 "enabled": false
	//             },
	//             "properties": {
	//                 "title": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 },
	//                 "content": {
	//                     "type": "string",
	//                     "analyzer": "ik_max_word",
	//                     "search_analyzer": "ik_max_word",
	//                     "include_in_all": "true"
	//                 }
	//             }
	//         }
	//     }
	// }`
	// for _, setting := range settings {
	// 	dest := *es + "/" + setting.indice + "/_mapping"
	// 	http.Post(dest, "application/json", strings.NewReader(setting.mapping))
	// 	log.Printf("###Create mapping for %v", setting.indice)
	// }

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
			url, exist := doc["url"]
			if !exist {
				log.Printf("No 'url' in line:%v", line)
				buffer.Reset()
				continue
			}
			indice := ""
			for _, v := range cfg.CP {
				if strings.Contains(url.(string), v.Pattern) {
					indice = v.Indice
					if indice == "" {
						indice = "default_data"
					}
					doc["weight"] = v.Weight
					doc["class"] = v.Class
					break
				}
			}
			if indice == "" {
				log.Printf("No match indice found for:%v", url)
				buffer.Reset()
				continue
			}
			b, _ := json.Marshal(doc)
			buffer.Reset()
			buffer.Write(b)
			dest := *es + "/" + indice + "/app/" + getMD5Hash(url.(string))
			res, err1 := http.Post(dest, "application/json", &buffer)
			if nil != err1 {
				log.Printf("Failed to es:%v", err1)
				return
			}
			log.Printf("###res:%v", indice)
			if nil != res.Body {
				io.Copy(ioutil.Discard, res.Body)
				res.Body.Close()
			}
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
