package shm

import (
	"testing"
)

const kTestShmSize = uint64(1 * 1024 * 1024)
const kTestShmPath = "/tmp/test_shm"
const kTestPrjId = uint8(123)

func TestShm(t *testing.T) {
	opt := &InitOptions{
		Path:      kTestShmPath,
		ProjId:    kTestPrjId,
		Exclusive: false,
		MaxSize:   kTestShmSize,
	}

	segment, err := Open(opt)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	copy(segment.Data[:], "hello,world")
	t.Logf("value:%s Len:%d", segment.Data[0:11], len(segment.Data))
	err = segment.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestReadShm(t *testing.T) {
	opt := &InitOptions{
		Path:     kTestShmPath,
		ProjId:   kTestPrjId,
		Readonly: true,
	}

	segment, err := Open(opt)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Logf("value:%s", segment.Data[0:11])
	err = segment.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}
func TestRmShm(t *testing.T) {
	opt := &InitOptions{
		Path:      kTestShmPath,
		ProjId:    kTestPrjId,
		Exclusive: false,
		MaxSize:   kTestShmSize,
	}

	segment, err := Open(opt)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	err = segment.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	err = segment.Remove()
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
}
