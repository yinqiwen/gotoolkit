package xjson

import (
	"encoding/json"
	"io"
	"reflect"
)

type XJson struct {
	//parent  *XJson
	value   interface{}
	members map[string]*XJson
	array   []*XJson
	invalid bool
}

var invalidXJson *XJson

func (x *XJson) Get(name string) *XJson {
	next, exist := x.members[name]
	if !exist {
		next = NewXJson()
		//next.parent = x
		x.members[name] = next
	}
	return next
}

func (x *XJson) RGet(name string) *XJson {
	next, exist := x.members[name]
	if !exist {
		return invalidXJson
	}
	return next
}
func (x *XJson) Add() *XJson {
	next := NewXJson()
	//next.parent = x
	x.array = append(x.array, next)
	return next
}
func (x *XJson) At(index int) *XJson {
	if len(x.array) < index || index < 0 {
		return invalidXJson
	}
	return x.array[index]
}
func (x *XJson) SetInt(v int64) {
	x.value = v
}
func (x *XJson) SetString(v string) {
	x.value = v
}

func (x *XJson) GetInt() int64 {
	if x.invalid {
		return -1
	}

	rv := reflect.ValueOf(x.value)
	switch rv.Type().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint())
	case reflect.Float32, reflect.Float64:
		return int64(rv.Float())
	}
	return -1
}

func (x *XJson) GetString() string {
	if x.invalid {
		return ""
	}
	if _, ok := x.value.(string); ok {
		return x.value.(string)
	}
	return ""
}

func (x *XJson) BuildJsonValue() interface{} {
	if len(x.members) > 0 {
		m := make(map[string]interface{})
		for k, v := range x.members {
			m[k] = v.BuildJsonValue()
		}
		return m
	}
	if len(x.array) > 0 {
		n := make([]interface{}, 0)
		for _, v := range x.array {
			n = append(n, v.BuildJsonValue())
		}
		return n
	}
	return x.value
}

func NewXJson() *XJson {
	x := new(XJson)
	x.members = make(map[string]*XJson)
	x.array = make([]*XJson, 0)
	x.invalid = false
	return x
}

func fillXJson(v interface{}, parent *XJson) {

	if m, ok := v.(map[string]interface{}); ok {
		for name, vv := range m {
			next := NewXJson()
			parent.members[name] = next
			fillXJson(vv, next)
		}
		return

	}
	if vs, ok := v.([]interface{}); ok {
		for _, vv := range vs {
			next := NewXJson()
			parent.array = append(parent.array, next)
			fillXJson(vv, next)
		}
		return
	}
	parent.value = v
}

func Decode(reader io.Reader) (*XJson, error) {
	var v interface{}
	err := json.NewDecoder(reader).Decode(&v)
	if nil != err {
		return nil, err
	}
	//log.Fatalf("###%T", v)
	x := NewXJson()
	fillXJson(v, x)
	return x, nil
}

func init() {
	invalidXJson = NewXJson()
	invalidXJson.invalid = true
}
