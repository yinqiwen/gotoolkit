package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yinqiwen/gotoolkit/iotools"
	"github.com/yinqiwen/gotoolkit/xjson"
)

var esClient = &http.Client{}

func getHttpBody(res *http.Response) string {
	if nil != res.Body {
		b, _ := ioutil.ReadAll(res.Body)
		return string(b)
	}
	return ""
}

func reindex(server string, remote string, index string, foreMerge bool, script string) error {
	log.Printf("Start reindex for server:%s with index:%s", server, index)
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
	//get alias
	addr := server + "/*/_alias/" + index
	res, err := esClient.Get(addr)
	err = getHttpErr(res, err)
	if nil != err {
		log.Printf("Get alias failed for %s:%s", index, server)
		return err
	}
	jsonDecoder := json.NewDecoder(res.Body)
	resJSON := make(map[string]interface{})
	err = jsonDecoder.Decode(&resJSON)
	res.Body.Close()
	if nil != err {
		return err
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
		return fmt.Errorf("No real index found for alias:%s", index)
	}
	i, err := strconv.Atoi(realIndex[len(realIndex)-1:])
	if nil != err {
		return err
	}

	//reindex next, it's a long running task
	nextIndex := fmt.Sprintf("%s%d", realIndex[0:len(realIndex)-1], 1-i)
	//delete next index, recreate it
	delReq, _ := http.NewRequest("DELETE", server+"/"+nextIndex, nil)
	res, err = esClient.Do(delReq)
	err = getHttpErr(res, err)
	if nil != err {
		log.Printf("Delete old index failed for %s:%s", index, server)
		return err
	}
	log.Printf("Server:%s DELETE next index:%s with response:%s", server, nextIndex, getHttpBody(res))

	//recreate index
	putReq, _ := http.NewRequest("PUT", server+"/"+nextIndex, nil)
	res, err = esClient.Do(putReq)
	err = getHttpErr(res, err)
	if nil != err {
		log.Printf("Recreate index failed for %s:%s", index, server)
		return err
	}
	log.Printf("Server:%s RECREATE next index:%s with response:%s", server, nextIndex, getHttpBody(res))
	reindexJson := xjson.NewXJson()
	reindexJson.Get("conflicts").SetString("proceed")
	reindexJson.Get("source").Get("remote").Get("host").SetString(remote)
	reindexJson.Get("source").Get("index").SetString(index)
	reindexJson.Get("dest").Get("index").SetString(nextIndex)
	if len(script) > 0 {
		ss, err := ioutil.ReadFile(script)
		if nil != err {
			log.Printf("Failed to read script from file:%s", script)
		} else {
			reindexJson.Get("script").Get("inline").SetString(string(ss))
			reindexJson.Get("script").Get("lang").SetString("painless")
		}

	}
	b, _ := json.Marshal(reindexJson.BuildJsonValue())
	log.Printf("Reindex request:%s", string(b))
	reindexFinished := false
	go func() {
		for !reindexFinished {
			time.Sleep(60 * time.Second)
			r, _ := esClient.Get(server + "/_tasks?detailed=true&actions=*reindex&pretty")
			if nil != r {
				resb, _ := ioutil.ReadAll(r.Body)
				r.Body.Close()
				log.Printf("Reindex task status:\n%s", string(resb))
			}

		}
	}()
	s1 := time.Now()
	res, err = esClient.Post(server+"/_reindex", "application/json", bytes.NewBuffer(b))
	reindexFinished = true
	err = getHttpErr(res, err)
	if nil != err {
		log.Printf("Reindex failed for %s:%s", index, server)
		return err
	}
	log.Printf("Server:%s cost %v to reindex index:%s with response:%s", server, time.Now().Sub(s1), nextIndex, getHttpBody(res))
	res.Body.Close()

	if foreMerge {
		s1 = time.Now()
		u := server + "/" + nextIndex + "/_forcemerge?max_num_segments=1"
		res, err = esClient.Post(u, "application/json", bytes.NewBufferString(""))
		err = getHttpErr(res, err)
		if nil != err {
			log.Printf("Force merge failed for %s", u)
			return err
		}
		res.Body.Close()
		log.Printf("Server:%s cost %v to force merge index:%s with response:%s", server, time.Now().Sub(s1), u, getHttpBody(res))
	}

	//switch index alias
	switchAlias := xjson.NewXJson()
	switchAlias.Get("actions").Add()
	switchAlias.Get("actions").Add()
	switchAlias.Get("actions").At(0).Get("remove").Get("index").SetString(realIndex)
	switchAlias.Get("actions").At(0).Get("remove").Get("alias").SetString(index)
	switchAlias.Get("actions").At(1).Get("add").Get("index").SetString(nextIndex)
	switchAlias.Get("actions").At(1).Get("add").Get("alias").SetString(index)
	b, _ = json.Marshal(switchAlias.BuildJsonValue())
	log.Printf("Server:%s switch index alias request:%s", server, string(b))
	res, err = esClient.Post(server+"/_aliases", "application/json", bytes.NewBuffer(b))
	err = getHttpErr(res, err)
	if nil != err {
		log.Printf("Switch index alias failed for %s:%s", index, server)
		return err
	}
	res.Body.Close()

	return nil
}

func main() {
	rindex := flag.String("rindex", "", "Indexes need to be reindex")
	servers := flag.String("server", "http://127.0.0.1:19200", "servers where index locate")
	remote := flag.String("remote", "http://127.0.0.1:19200", "remote index data")
	flock := flag.String("flock", "/ceph/es_tools", "ES tools file lock")
	touch := flag.String("touch", "/ceph/es_indexs/ts", "Touch file after process.")
	merge := flag.Bool("merge", false, "merge index after reindex")
	script := flag.String("script", "", "Reindex script")
	flag.Parse()

	iotools.InitLogger([]string{"stdout", "es_reindex_" + *rindex + ".log"})
	err := iotools.NewFileLock(*flock).Lock()
	for nil != err {
		log.Printf("Lock acuire failed:%v", err)
		time.Sleep(10 * time.Second)
		err = iotools.NewFileLock(*flock).Lock()
	}

	reindexs := strings.Split(*rindex, ",")
	ss := strings.Split(*servers, ",")

	var wg sync.WaitGroup
	for _, index := range reindexs {
		for _, server := range ss {
			wg.Add(1)
			s := server
			idx := index
			go func() {
				err := reindex(s, *remote, idx, *merge, *script)
				if nil != err {
					log.Printf("Error:%v for server:%s index:%s", err, s, idx)
				}
				wg.Done()
			}()
		}
	}
	wg.Wait()
	if len(*touch) > 0 {
		os.Chtimes(*touch, time.Now(), time.Now())
	}
}
