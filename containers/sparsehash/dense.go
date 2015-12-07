package sparsehash

//"fmt"

const (
	KEY_EMPTY_INDEX             = int64(0)
	KEY_DELETED_INDEX           = int64(-1)
	ILLEGAL_BUCKET              = int64(-3)
	HT_MIN_BUCKETS              = int64(4)
	HT_DEFAULT_STARTING_BUCKETS = 32
)

type keyEntry struct {
	key      interface{}
	valIndex int64
}

type entryIndex struct {
	keyIndex int64
	valIndex int64
}

type denseHashmap struct {
	keys        []keyEntry
	vals        []interface{}
	numDeleted  int64
	numElements int64

	enlargeThreshold int64
	shrinkThreshold  int64
	enlargeFactor    float64
	shrinkFactor     float64
}

func NewDenseHashmap(enlargeFactor, shrinkFactor float64) HashMap {
	hmap := new(denseHashmap)
	hmap.keys = make([]keyEntry, HT_DEFAULT_STARTING_BUCKETS)
	hmap.vals = make([]interface{}, 0)
	hmap.setEnlargeShrinkFactors(enlargeFactor, shrinkFactor)
	return hmap
}

func (dense *denseHashmap) setEnlargeShrinkFactors(enlargeFactor, shrinkFactor float64) {
	dense.enlargeFactor = enlargeFactor
	dense.shrinkFactor = shrinkFactor
	dense.enlargeThreshold = int64(float64(len(dense.keys)) * enlargeFactor)
	dense.shrinkThreshold = int64(float64(len(dense.keys)) * shrinkFactor)
}

func minBuckets(numElts, minBucketsWanted int64, enlargeFactor float64) int64 {
	sz := HT_MIN_BUCKETS // min buckets allowed
	for sz < minBucketsWanted || numElts >= int64(float64(sz)*enlargeFactor) {
		if sz*2 < sz {
			panic("resize overflow")
		}
		sz = sz * 2
	}
	return sz
}
func (dense *denseHashmap) tryEnlarge() bool {
	//shouldResize := false
	newElementSize := dense.numElements + 1
	//fmt.Printf("###%d %d\n", newElementSize, dense.enlargeThreshold)
	if newElementSize > HT_MIN_BUCKETS && newElementSize <= dense.enlargeThreshold {
		return false
	}
	neededSize := minBuckets(dense.numElements+dense.numDeleted, 0, dense.enlargeFactor)
	if neededSize < int64(len(dense.keys)) {
		return false
	}
	resizeTo := minBuckets(newElementSize, 0, dense.enlargeFactor)
	if resizeTo < neededSize {
		target := int64(float64(resizeTo*2) * dense.shrinkFactor)
		if newElementSize >= target {
			resizeTo = resizeTo * 2
		}
	}
	dense.resize(resizeTo)
	return true
}

func (dense *denseHashmap) tryShrink() bool {
	if dense.shrinkThreshold > 0 && dense.numElements < dense.shrinkThreshold && len(dense.keys) > HT_DEFAULT_STARTING_BUCKETS {
		sz := len(dense.keys) / 2
		for sz > HT_DEFAULT_STARTING_BUCKETS && dense.numElements < int64(float64(sz)*dense.shrinkFactor) {
			sz = sz / 2
		}
		dense.resize(int64(sz))
		return true
	}
	return false
}

func (dense *denseHashmap) resize(newSize int64) {
	newKeys := make([]keyEntry, newSize)
	var newVals []interface{}
	shrinkVals := false
	if dense.numElements > HT_MIN_BUCKETS && dense.numElements < int64(len(dense.vals)/2) {
		shrinkVals = true
		newVals = make([]interface{}, dense.numElements)
	}
	bucketCountMinusOne := uint64(newSize - 1)
	newValIndex := int64(1)
	for i := 0; i < len(dense.keys); i++ {
		if dense.keys[i].valIndex <= 0 { // no value set
			continue
		}
		numProbes := int64(0) // how many times we've probed
		bucknum := int64(hash(dense.keys[i].key) & bucketCountMinusOne)
		for {
			if newKeys[bucknum].valIndex == 0 { //empty entry
				break
			}
			numProbes++
			if numProbes >= newSize {
				panic("Hashtable is full: an error in key_equal<> or hash<>")
			}
			bucknum = (bucknum + numProbes) & int64(bucketCountMinusOne)
		}
		newKeys[bucknum] = dense.keys[i]
		if shrinkVals {
			newKeys[bucknum].valIndex = newValIndex
			newVals[newValIndex-1] = dense.vals[dense.keys[i].valIndex-1]
			newValIndex++
		}
	}
	dense.numDeleted = 0
	dense.keys = newKeys
	if shrinkVals {
		dense.vals = newVals
	}
	dense.setEnlargeShrinkFactors(dense.enlargeFactor, dense.shrinkFactor)
}

func (dense *denseHashmap) findPosition(key interface{}) (int64, int64) {
	hashCode := hash(key)
	numProbes := int64(0) // how many times we've probed
	bucketCountMinusOne := int64(len(dense.keys) - 1)
	bucknum := int64(hashCode & uint64(bucketCountMinusOne))
	insertPos := ILLEGAL_BUCKET // where we would insert
	for {

		if dense.keys[bucknum].valIndex == KEY_EMPTY_INDEX {
			if insertPos == ILLEGAL_BUCKET { // found no prior place to insert
				return ILLEGAL_BUCKET, bucknum
			}
			return ILLEGAL_BUCKET, insertPos
		} else if dense.keys[bucknum].valIndex == KEY_DELETED_INDEX {
			if insertPos == ILLEGAL_BUCKET {
				insertPos = bucknum
			}
		} else if equals(key, dense.keys[bucknum].key) {
			return bucknum, ILLEGAL_BUCKET
		}
		numProbes++
		//bucknum = (bucknum + JUMP_(key, num_probes)) & bucket_count_minus_one;
		bucknum = (bucknum + numProbes) & bucketCountMinusOne
		if numProbes >= int64(len(dense.keys)) {
			panic("Hashtable is full: an error in key_equal<> or hash<>")
		}
	}
}

func (dense *denseHashmap) Put(key interface{}, value interface{}) (bool, interface{}) {
	dense.tryEnlarge()
	pos, insertAt := dense.findPosition(key)
	if insertAt == ILLEGAL_BUCKET {
		return false, dense.vals[dense.keys[pos].valIndex-1]
	}

	dense.numElements++
	dense.keys[insertAt].key = key
	if dense.keys[insertAt].valIndex == KEY_DELETED_INDEX {
		dense.numDeleted--
		dense.vals[dense.keys[insertAt].valIndex-1] = value
	} else {
		dense.vals = append(dense.vals, value)
		dense.keys[insertAt].valIndex = int64(len(dense.vals))
	}
	//fmt.Printf("####insert at %d %v\n", insertAt, dense.vals[dense.keys[insertAt].valIndex-1])
	return true, nil
}

func (dense *denseHashmap) Del(key interface{}) (bool, interface{}) {
	pos, _ := dense.findPosition(key)
	if pos < 0 {
		return false, nil
	}
	valIndex := dense.keys[pos].valIndex - 1
	v := dense.vals[valIndex]
	dense.vals[valIndex] = nil
	dense.keys[pos].key = nil
	dense.keys[pos].valIndex = KEY_DELETED_INDEX
	dense.numDeleted++
	dense.numElements--
	dense.tryShrink()
	return true, v
}

func (dense *denseHashmap) Get(key interface{}) interface{} {
	pos, _ := dense.findPosition(key)
	if pos < 0 {
		return nil
	}
	//fmt.Printf("####get at %d %d \n", pos, dense.keys[pos].valIndex-1)
	return dense.vals[dense.keys[pos].valIndex-1]
}

func (dense *denseHashmap) Clear() {
	dense.keys = make([]keyEntry, HT_DEFAULT_STARTING_BUCKETS)
	dense.vals = make([]interface{}, HT_DEFAULT_STARTING_BUCKETS)
	dense.numDeleted = 0
	dense.numElements = 0
}

func (dense *denseHashmap) Size() int {
	return int(dense.numElements)
}
