package iotools

import (
	"os"
	"testing"
	"time"
)

func TestRotate(t *testing.T) {
	file := "./test.log"
	rfile := RotateFile{
		Path:            file,
		MaxBackupIndex:  2,
		MaxFileSize:     1024 * 1024,
		SyncBytesPeriod: 1024 * 1024,
	}
	content := []byte("hello, world!!!!\n")
	for i := 0; i < 100000; i++ {
		rfile.Write(content)
		//time.Sleep(1)
		if i%10 == 1 {
			os.Remove(file)
		}
	}
}
