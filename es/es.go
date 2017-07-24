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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yinqiwen/gotoolkit/iotools"
	"github.com/yinqiwen/gotoolkit/xjson"
)

type extensionValues map[string]string

func (i *extensionValues) String() string {
	return "my string representation"
}

func (i *extensionValues) Set(value string) error {
	ss := strings.SplitN(value, "=", 2)
	if len(ss) != 2 {
		return fmt.Errorf("Invalid format for %s %d", value, len(ss))
	}
	(*i)[ss[0]] = ss[1]
	return nil
}

type DataSourceSetting struct {
	Pattern string
	CP      string
	C1      string //class1
	C2      string //class2
}

func getMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

type ESConfig struct {
	ES            []string
	CP            []DataSourceSetting
	Indice        string
	Type          string
	Ext           extensionValues
	Touch         string
	FLock         string
	DefaultField  map[string]interface{}
	Rebuild       bool
	rebuildIndice []string
	realIndice    []string
}

var gcfg ESConfig
var startTime time.Time
var processCount int64
var limitCount int64
var concurrentCount int32
var maxConcurrent int32
var invalidDateDocCount int32

var errLogCount int64

var esClient = &http.Client{}

func deleteIfNotInt(doc map[string]interface{}, field string) {
	v, exist := doc[field]
	if exist {
		nu, ok := v.(json.Number)
		if ok {
			_, err := nu.Int64()
			ok = nil == err
		} else {
			ns := v.(string)
			iv, err := strconv.Atoi(ns)
			ok = nil == err
			if ok {
				delete(doc, field)
				doc[field] = iv
			}
		}
		if !ok {
			if atomic.AddInt64(&errLogCount, 1)%1000 == 1 {
				log.Printf("[WARN]Invalid field:%v with wrong type value:%v for doc:%v", field, doc[field], doc["id"])
			}
			delete(doc, field)
		}
	}
}

func postData(line string) error {
	doc := make(map[string]interface{})
	d := json.NewDecoder(strings.NewReader(line))
	d.UseNumber()
	err := d.Decode(&doc)
	if nil != err {
		log.Printf("Failed to unmarshal json:%s to config for reason:%v", line, err)
		return err
	}
	idstr := ""
	_, deleteDoc := doc["delete"]

	url, urlexist := doc["url"]
	if !urlexist && !deleteDoc {
		return fmt.Errorf("No 'url' in line:%v", line)
	}
	urlstr := url.(string)
	id, exist := doc["id"]
	if !exist {
		if deleteDoc {
			return nil
		}
		idstr = getMD5Hash(urlstr)
	} else {
		idstr = id.(string)
	}

	if !deleteDoc {
		_, exist = doc["title"]
		if !exist {
			return fmt.Errorf("No 'title' in line:%v", line)
			//return false, nil
		}
		existCp := ""
		if tmp, cpExist := doc["cp"]; cpExist {
			existCp = tmp.(string)
		}
		var cp DataSourceSetting
		//cpMatch := false
		if len(existCp) == 0 {
			for _, v := range gcfg.CP {
				if strings.Contains(urlstr, v.Pattern) || v.Pattern == "*" {
					cp = v
					doc["cp"] = cp.CP
					if _, eleExist := doc["c1"]; !eleExist {
						if len(cp.C1) > 0 {
							doc["c1"] = cp.C1
						}
					}
					if _, eleExist := doc["c2"]; !eleExist {
						if len(cp.C2) > 0 {
							doc["c2"] = cp.C2
						}
					}
					//cpMatch = true
					break
				}
			}
		}

		// if !cpMatch && len(existCp) == 0 {
		// 	doc["cp"] = "unknown"
		// 	doc["c1"] = "unknown"
		// 	doc["c2"] = "unknown"
		// 	log.Printf("[WARN]Unmatched pattern for line with id:%v & url:%v", idstr, doc["url"])
		// 	//return fmt.Errorf("No match pattern found for:%v", doc["url"])
		// }
		for k, v := range gcfg.Ext {
			doc[k] = v
		}

		if len(gcfg.DefaultField) > 0 {
			for k, v := range gcfg.DefaultField {
				if _, exist := doc[k]; !exist {
					doc[k] = v
				}
			}
		}
		delete(doc, "id")
		if date, exist := doc["date"]; exist {
			if date == "0000-00-00 00:00:00" {
				delete(doc, "date")
			} else {
				_, dateErr := time.Parse("2006-01-02 15:04:05", date.(string))
				if nil != dateErr {
					atomic.AddInt32(&invalidDateDocCount, 1)
					log.Printf("Invalid 'date:%v' for doc:%v with url %v", date, idstr, doc["url"])
					delete(doc, "date")
				}
			}
		}
	}

	for i, esAddr := range gcfg.ES {
		var httpReq *http.Request
		var dest string
		if len(gcfg.rebuildIndice) > 0 {
			dest = esAddr + "/" + gcfg.rebuildIndice[i] + "/" + gcfg.Type + "/" + idstr + "?timeout=1m"
		} else {
			dest = esAddr + "/" + gcfg.Indice + "/" + gcfg.Type + "/" + idstr + "?timeout=1m"
		}

		if deleteDoc {
			delete(doc, "delete")
			dest = esAddr + "/" + gcfg.Indice + "/" + gcfg.Type + "/" + idstr
			httpReq, err = http.NewRequest("DELETE", dest, nil)
			if err != nil {
				return err
			}
		} else {
			b, _ := json.Marshal(doc)
			httpReq, err = http.NewRequest("POST", dest, bytes.NewBuffer(b))
			if err != nil {
				return err
			}
			httpReq.Header.Set("Content-Type", "application/json")
		}

		res, err1 := esClient.Do(httpReq)
		//res, err1 := esClient.Post(dest, "application/json", bytes.NewBuffer(b))
		if nil != err1 {
			return err1
		}
		if res.StatusCode >= 500 {
			resb, _ := ioutil.ReadAll(res.Body)
			res.Body.Close()
			log.Printf("##Recv error %d:%s for line:%s", res.StatusCode, string(resb), line)
		} else {
			if nil != res.Body {
				io.Copy(ioutil.Discard, res.Body)
				res.Body.Close()
			}
		}
	}

	atomic.AddInt64(&processCount, 1)
	if processCount%1000 == 0 {
		log.Printf("#####Cost %v to put %d records", time.Now().Sub(startTime), processCount)
	}
	return nil
}

func getHttpBody(res *http.Response) string {
	if nil != res.Body {
		b, _ := ioutil.ReadAll(res.Body)
		return string(b)
	}
	return ""
}

func main() {
	gcfg.Ext = make(map[string]string)

	file := flag.String("file", "", "Specify input file")
	format := flag.String("fformat", "", "Specify input file regex format")
	cfile := flag.String("config", "config.json", "Specify config file")
	count := flag.Int64("count", -1, "Specify total process count.")
	concurrent := flag.Int("concurrent", 1, "Specify max concurrent worker.")
	bak := flag.String("backup", "./bakup", "Specify bacup dir")
	inputDir := flag.String("dir", "./", "Specify bacup dir")
	flag.Var(&gcfg.Ext, "X", "Some description for this param.")

	flag.Parse()

	getHttpErr := func(res *http.Response, err error) error {
		if nil != err {
			return err
		}
		if res.StatusCode != 200 {
			resb, _ := ioutil.ReadAll(res.Body)
			res.Body.Close()
			return fmt.Errorf("%s", string(resb))
		}
		return nil
	}

	if len(*bak) > 0 {
		os.Mkdir(*bak, os.ModePerm)
	}

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

	inputFiles := *file
	var fileNames []string
	if len(inputFiles) == 0 {
		iotools.InitLogger([]string{gcfg.Indice + ".log"})
		files, _ := ioutil.ReadDir(*inputDir)
		for _, f := range files {
			if !f.IsDir() && !strings.HasSuffix(f.Name(), ".cksm") && !strings.HasSuffix(f.Name(), ".done") {
				match, err := regexp.MatchString(*format, f.Name())
				if nil == err && match {
					fileNames = append(fileNames, f.Name())
				}
			}
		}
	} else {
		iotools.InitLogger([]string{"stdout", gcfg.Indice + ".log"})
		fileNames = strings.Split(inputFiles, ",")
	}

	err = iotools.NewFileLock(gcfg.FLock).Lock()
	for nil != err {
		log.Printf("Lock acuire failed:%v", err)
		time.Sleep(10 * time.Second)
		err = iotools.NewFileLock(gcfg.FLock).Lock()
	}

	var tmpFileNames []string
	for _, fname := range fileNames {
		cksm := fname + ".cksm"
		done := fname + ".done"
		if _, err := os.Stat(cksm); os.IsNotExist(err) {
			log.Printf("No cksm file:%s for %s", cksm, fname)
			continue
		}
		if _, err := os.Stat(done); os.IsNotExist(err) {
			tmpFileNames = append(tmpFileNames, fname)
		} else {
			log.Printf("File:%s is already done before.", fname)
			continue
		}
	}
	fileNames = tmpFileNames
	if len(fileNames) == 0 {
		log.Printf("No available files for %v", fileNames)
		return
	}
	log.Printf("Start to load data from files:%v", fileNames)

	tr := &http.Transport{
		MaxIdleConnsPerHost: 2 * int(*concurrent),
	}
	esClient.Transport = tr
	limitCount = *count

	if gcfg.Rebuild {
		gcfg.rebuildIndice = make([]string, 0)
		gcfg.realIndice = make([]string, 0)
		for _, server := range gcfg.ES {
			addr := server + "/*/_alias/" + gcfg.Indice
			res, err := esClient.Get(addr)
			err = getHttpErr(res, err)
			if nil != err {
				log.Printf("Get alias failed for %s:%s", gcfg.Indice, server)
				return
			}
			jsonDecoder := json.NewDecoder(res.Body)
			resJSON := make(map[string]interface{})
			err = jsonDecoder.Decode(&resJSON)
			res.Body.Close()
			if nil != err {
				log.Printf("Get alias failed for %s:%s", gcfg.Indice, server)
				return
			}
			realIndex := ""
			for k, v := range resJSON {
				vm := v.(map[string]interface{})
				if alias, exist := vm["aliases"]; exist {
					am := alias.(map[string]interface{})
					if len(am) > 0 {
						realIndex = k
						break
					}
				}
			}
			if len(realIndex) == 0 {
				log.Printf("No real index found for alias:%s", gcfg.Indice)
				return
			}
			gcfg.realIndice = append(gcfg.realIndice, realIndex)
			i, err := strconv.Atoi(realIndex[len(realIndex)-1:])
			if nil != err {
				log.Printf("Invalid indice type with no idx %s", realIndex)
				return
			}
			//reindex next, it's a long running task
			nextIndex := fmt.Sprintf("%s%d", realIndex[0:len(realIndex)-1], 1-i)
			//delete next index, recreate it
			delReq, _ := http.NewRequest("DELETE", server+"/"+nextIndex, nil)
			res, err = esClient.Do(delReq)
			err = getHttpErr(res, err)
			if nil != err {
				log.Printf("Delete old index failed for %s:%s", gcfg.Indice, server)
				return
			}
			log.Printf("Server:%s DELETE next index:%s with response:%s", server, nextIndex, getHttpBody(res))

			//recreate index
			putReq, _ := http.NewRequest("PUT", server+"/"+nextIndex, nil)
			res, err = esClient.Do(putReq)
			err = getHttpErr(res, err)
			if nil != err {
				log.Printf("Recreate index failed for %s:%s", gcfg.Indice, server)
				return
			}
			log.Printf("Server:%s RECREATE next index:%s with response:%s", server, nextIndex, getHttpBody(res))
			gcfg.rebuildIndice = append(gcfg.rebuildIndice, nextIndex)
		}

	}

	startTime = time.Now()
	var wg sync.WaitGroup
	taskChan := make(chan string, *concurrent)

	for i := 0; i < *concurrent; i++ {
		wg.Add(1)
		go func(idx int) {
			for line := range taskChan {
				if len(line) == 0 {
					break
				}
				err := postData(line)
				if nil != err {
					log.Printf("Failed to post data for reason:%v", err)
				}
			}
			wg.Done()
			log.Printf("Task:%d exit.", idx)
		}(i)
	}
	lineCount := int64(0)

	for _, fname := range fileNames {
		var f *os.File
		if f, err = os.Open(fname); err != nil {
			log.Printf("###Error:%v", err)
			continue
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
		log.Printf("#####Cost %v to load file:%s & put %d records into es.", time.Now().Sub(startTime), fname, processCount)
		if limitCount > 0 && lineCount >= limitCount {
			break
		}
		done := fname + ".done"
		f.Close()
		os.Create(done)
		if len(*bak) > 0 {
			os.Rename(done, *bak+"/"+done)
			os.Rename(fname, *bak+"/"+fname)
			cksm := fname + ".cksm"
			os.Rename(cksm, *bak+"/"+cksm)
		}
	}
	close(taskChan)
	wg.Wait()
	log.Printf("#####Cost %v to put %d records into es.", time.Now().Sub(startTime), processCount)
	if invalidDateDocCount > 0 {
		log.Printf("####Total %d doc with invalid 'date' field.", invalidDateDocCount)
	}

	if gcfg.Rebuild {
		for i, server := range gcfg.ES {
			//switch index alias
			switchAlias := xjson.NewXJson()
			switchAlias.Get("actions").Add()
			switchAlias.Get("actions").Add()
			switchAlias.Get("actions").At(0).Get("remove").Get("index").SetString(gcfg.realIndice[i])
			switchAlias.Get("actions").At(0).Get("remove").Get("alias").SetString(gcfg.Indice)
			switchAlias.Get("actions").At(1).Get("add").Get("index").SetString(gcfg.rebuildIndice[i])
			switchAlias.Get("actions").At(1).Get("add").Get("alias").SetString(gcfg.Indice)
			b, _ = json.Marshal(switchAlias.BuildJsonValue())
			log.Printf("Server:%s switch index alias request:%s", server, string(b))
			res, err := esClient.Post(server+"/_aliases", "application/json", bytes.NewBuffer(b))
			err = getHttpErr(res, err)
			if nil != err {
				log.Printf("Switch index alias failed for %s:%s", gcfg.Indice, server)
				return
			}
			res.Body.Close()
		}

	}

	if len(gcfg.Touch) > 0 {
		os.Create(gcfg.Touch)
		os.Chtimes(gcfg.Touch, time.Now(), time.Now())
	}

}
