package xjson

type XJson struct {
	//parent  *XJson
	value   interface{}
	members map[string]*XJson
	array   []*XJson
}

func (x *XJson) Get(name string) *XJson {
	next, exist := x.members[name]
	if !exist {
		next = NewXJson()
		//next.parent = x
		x.members[name] = next
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
	return x.array[index]
}
func (x *XJson) SetInt(v int64) {
	x.value = v
}
func (x *XJson) SetString(v string) {
	x.value = v
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
	return x
}
