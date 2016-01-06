package gstat

import (
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

//Track common statistic track task interface
type Track interface {
	Dump(name string, wr io.Writer)
}

//CountTrack holds a int64 counter
type CountTrack struct {
	count int64
}

//Add on the counter
func (rec *CountTrack) Add(cost int64) int64 {
	if cost == 0 {
		return atomic.LoadInt64(&rec.count)
	}
	return atomic.AddInt64(&rec.count, cost)
}

//Set the counter with given value
func (rec *CountTrack) Set(v int64) {
	rec.count = v
}

//Get the counter value
func (rec *CountTrack) Get() int64 {
	return rec.count
}

//Dump statistics recrod into the writer
func (rec *CountTrack) Dump(name string, wr io.Writer) {
	fmt.Fprintf(wr, "%s: %d\n", name, rec.count)
}

//CountRefTrack holds a counter refernce
type CountRefTrack struct {
	Callback func() int64
}

//Get the counter value
func (rec *CountRefTrack) Get() int64 {
	count := int64(0)
	if nil != rec.Callback {
		count = rec.Callback()
	}
	return count
}

//Dump statistics recrod into the writer
func (rec *CountRefTrack) Dump(name string, wr io.Writer) {
	fmt.Fprintf(wr, "%s: %d\n", name, rec.Get())
}

//CostRange holds a [min, max] range value
type CostRange struct {
	Min int64
	Max int64
}

//CostRecord holds a cost record
type CostRecord struct {
	CostRange
	Cost  int64
	Count int64
}

//CostTrack holds cost range statistic recrods
type CostTrack struct {
	//rangeSepc []CostRange
	recs []CostRecord //recs[0] holds cumulate record
	lk   sync.Mutex
}

//SetCostRanges init cost range setting
func (rec *CostTrack) SetCostRanges(ranges []CostRange) {
	rec.recs = make([]CostRecord, len(ranges)+1)
	for i := 0; i < len(ranges); i++ {
		rec.recs[i+1].Min = ranges[i].Min
		rec.recs[i+1].Max = ranges[i].Max
	}
}

//AddCost add a cost statistic record
func (rec *CostTrack) AddCost(cost int64) {
	if len(rec.recs) == 0 {
		rec.SetCostRanges(nil)
	}
	atomic.AddInt64(&rec.recs[0].Cost, cost)
	atomic.AddInt64(&rec.recs[0].Count, 1)
	for i := 1; i < len(rec.recs); i++ {
		if cost >= rec.recs[i].Min && cost <= rec.recs[i].Max {
			atomic.AddInt64(&rec.recs[i+1].Cost, cost)
			atomic.AddInt64(&rec.recs[i+1].Count, 1)
			return
		}
	}
}

//Clear all counts
func (rec *CostTrack) Clear() {
	for i := 0; i < len(rec.recs); i++ {
		rec.recs[i].Count = 0
		rec.recs[i].Cost = 0
	}
}

//Get return all records
func (rec *CostTrack) Get() []CostRecord {
	return rec.recs
}

//Dump statistics recrod into the writer
func (rec *CostTrack) Dump(name string, wr io.Writer) {
	if len(rec.recs) == 0 || rec.recs[0].Count == 0 {
		return
	}
	allCount := int64(0)
	for i := 0; i < len(rec.recs); i++ {
		if rec.recs[i].Count == 0 {
			continue
		}
		var field string
		if i == 0 {
			field = "all"
			allCount = rec.recs[0].Count
		} else {
			if rec.recs[i-1].Max == math.MaxInt64 {
				field = fmt.Sprintf("range[%d-]", rec.recs[i-1].Min)
			} else {
				field = fmt.Sprintf("range[%d-%d]", rec.recs[i-1].Min, rec.recs[i-1].Max)
			}
		}
		countKey := fmt.Sprintf("%s:%s:count", name, field)
		costKey := fmt.Sprintf("%s:%s:cost", name, field)
		fmt.Fprintf(wr, "%s: %d[%.4f%%]\n", countKey, rec.recs[i].Count, (float64(rec.recs[i].Count)/float64(allCount))*100)
		fmt.Fprintf(wr, "%s: %d\n", costKey, rec.recs[i].Cost)
		rec.recs[i].Count = 0
		rec.recs[i].Cost = 0
	}
}

//QPSTrack holds message QPS type record
type QPSTrack struct {
	name           string
	qpsSamples     [16]int64
	qpsSampleIdx   int
	lastMsgCount   int64
	lastSampleTime int64
	msgCount       int64
}

func (q *QPSTrack) trackQPSPerSecond() {
	now := time.Now().UnixNano()
	var qps int64
	if q.lastSampleTime == 0 {
		q.lastSampleTime = now
	}
	if now > q.lastSampleTime {
		qps = ((q.msgCount - q.lastMsgCount) * 1000000000) / (now - q.lastSampleTime)
	}
	q.qpsSamples[q.qpsSampleIdx] = qps
	q.qpsSampleIdx = (q.qpsSampleIdx + 1) % len(q.qpsSamples)
	q.lastMsgCount = q.msgCount
	q.lastSampleTime = now
}

//GetCount return total count
func (q *QPSTrack) GetCount() int64 {
	return q.msgCount
}

//QPS return estimated period QPS
func (q *QPSTrack) QPS() int {
	sum := int64(0)
	for i := 0; i < len(q.qpsSamples); i++ {
		sum += q.qpsSamples[i]
	}
	return int(sum) / len(q.qpsSamples)
}

//IncMsgCount add msg count
func (q *QPSTrack) IncMsgCount(inc int64) {
	atomic.AddInt64(&q.msgCount, inc)
}

//Dump statistics recrod into the writer
func (q *QPSTrack) Dump(name string, wr io.Writer) {
	countKey := fmt.Sprintf("%sCount", name)
	qpsKey := fmt.Sprintf("%sQPS", name)
	fmt.Fprintf(wr, "%s: %d\n", countKey, q.GetCount())
	qps := q.QPS()
	fmt.Fprintf(wr, "%s: %d\n", qpsKey, qps)
}

var statTracks = make(map[string]Track)
var qpsTracks = make([]*QPSTrack, 0)
var statTrackMutex sync.Mutex

//AddStatTrack register a gstat track
func AddStatTrack(name string, track Track) error {
	statTrackMutex.Lock()
	defer statTrackMutex.Unlock()
	if _, ok := statTracks[name]; ok {
		return fmt.Errorf("Duplicate stat track name:%s", name)
	}
	statTracks[name] = track
	if qps, ok := track.(*QPSTrack); ok {
		qps.name = name
		qpsTracks = append(qpsTracks, qps)
	} else if cost, ok := track.(*CostTrack); ok {
		if len(cost.recs) == 0 {
			cost.SetCostRanges(nil)
		}
	}
	return nil
}

//Get return a gstat track by given name
func Get(name string) Track {
	statTrackMutex.Lock()
	defer statTrackMutex.Unlock()
	if track, ok := statTracks[name]; ok {
		return track
	}
	return nil
}

//GetAll return all registed gstat track
func GetAll() map[string]Track {
	ret := make(map[string]Track)
	statTrackMutex.Lock()
	defer statTrackMutex.Unlock()
	for name, track := range statTracks {
		ret[name] = track
	}
	return ret
}

//PrintQPS print qps record into writer
func PrintQPS(writer io.Writer) {
	for _, qps := range qpsTracks {
		qps.Dump(qps.name, writer)
	}
}

//PrintCounts print counter record into writer
func PrintCounts(writer io.Writer) {
	var names []string
	for name, track := range statTracks {
		switch track.(type) {
		case *CountTrack, *CountRefTrack:
			//track.Dump(name, writer)
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		statTracks[name].Dump(name, writer)
	}
}

//PrintAll print all records into writer
func PrintAll(writer io.Writer) {
	names := make([]string, len(statTracks))
	i := 0
	for name := range statTracks {
		//track.Dump(name, writer)
		names[i] = name
		i++
	}
	sort.Strings(names)
	for _, name := range names {
		statTracks[name].Dump(name, writer)
	}
}

func routine() {
	oneSecTicker := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-oneSecTicker.C:
			for _, q := range qpsTracks {
				q.trackQPSPerSecond()
			}
		}
	}
}

func init() {
	go routine()
}
