package main

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yinqiwen/gotoolkit/iotools"
)

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
	ES    []string
	Index string
}

var gcfg ESConfig
var startTime time.Time
var processCount int64
var limitCount int64
var concurrentCount int32
var maxConcurrent int32
var invalidDateDocCount int32

var esClient = &http.Client{}

func deleteDoc(line string) error {
	line = strings.TrimSpace(line)
	if len(line) == 0 {
		return nil
	}
	for _, addr := range gcfg.ES {
		dest := "http://" + addr + "/" + gcfg.Index + "/" + line + "?timeout=1m"
		httpReq, err := http.NewRequest("DELETE", dest, nil)
		var res *http.Response
		if err == nil {
			res, err = esClient.Do(httpReq)
		}
		if nil != res {
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
		if nil != err {
			return err
		}
	}
	atomic.AddInt64(&processCount, 1)
	if processCount%10000 == 0 {
		log.Printf("#####Cost %v to put %d records", time.Now().Sub(startTime), processCount)
	}
	return nil
}

func main() {
	file := flag.String("file", "", "Specify input file")
	del := flag.Bool("delete", false, "delete id in file")
	addr := flag.String("addr", "localhost:9200", "Specify elasticsearch addr.")
	index := flag.String("index", "incice/type", "Specify target incide&type.")
	concurrent := flag.Int("concurrent", 1, "Specify max concurrent worker.")
	flag.Parse()

	inputFiles := *file
	iotools.InitLogger([]string{"stdout", "es_cmd.log"})
	fileNames := strings.Split(inputFiles, ",")
	gcfg.ES = strings.Split(*addr, ",")
	gcfg.Index = *index
	if len(fileNames) == 0 {
		log.Printf("No available files for %v", inputFiles)
		return
	}

	tr := &http.Transport{
		MaxIdleConnsPerHost: 2 * int(*concurrent),
	}
	esClient.Transport = tr

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
				var err error
				if *del {
					err = deleteDoc(line)
				}

				if nil != err {
					log.Printf("Failed to post data for reason:%v", err)
				}
			}
			wg.Done()
			log.Printf("Task:%d exit.", idx)
		}(i)
	}
	lineCount := int64(0)
	var err error
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
		os.Create(fname + ".done")
	}
	close(taskChan)
	wg.Wait()
	log.Printf("#####Cost %v to put %d records into es.", time.Now().Sub(startTime), processCount)

}
