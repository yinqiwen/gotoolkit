package xjson

import (
	"bytes"
	"testing"
)

func TestXYZ(t *testing.T) {
	disableJSON := "{\"transient\":{\"cluster.routing.allocation.enable\":\"none\", \"a\":1}}"
	x, err := Decode(bytes.NewBufferString(disableJSON))
	if nil != err {
		t.Fatalf("%v", err)
	}
	str := x.RGet("transient").RGet("cluster.routing.allocation.enable").GetString()
	if str != "none" {
		t.Fatalf("###%v### %v", str, x.members)
	}
	iv := x.RGet("transient").RGet("a").GetInt()
	if iv != 1 {
		t.Fatalf("###%v### %v", str, x.members)
	}
}
