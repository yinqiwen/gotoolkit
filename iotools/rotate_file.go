package iotools

import (
	"fmt"
	"os"
	"sync"
)

const MaxRotateFileSize = int64(1024 * 1024 * 1024)

type RotateFile struct {
	Path            string
	MaxBackupIndex  int
	MaxFileSize     int64
	SyncBytesPeriod int
	openFlag        int
	openPerm        os.FileMode
	fileSize        int64
	file            *os.File
	lk              sync.Mutex
}

func (rfile *RotateFile) Open(path string, flag int, perm os.FileMode) error {
	if nil != rfile.file {
		return fmt.Errorf("Already open file:%s", rfile.Path)
	}
	rfile.Path = path
	rfile.openFlag = flag
	rfile.openPerm = perm
	return rfile.reopen()
}

func (rfile *RotateFile) Close() error {
	rfile.lk.Lock()
	defer rfile.lk.Unlock()
	return rfile.close()
}

func (rfile *RotateFile) close() error {
	if nil == rfile.file {
		return nil
	}
	err := rfile.file.Close()
	if nil == err {
		rfile.file = nil
		rfile.fileSize = 0
	}
	return err
}

func (rfile *RotateFile) reopen() error {
	if len(rfile.Path) == 0 {
		return fmt.Errorf("Empty file name to open")
	}
	err := rfile.close()
	if 0 == rfile.openFlag {
		rfile.openFlag = os.O_CREATE | os.O_RDWR | os.O_APPEND
	}
	if 0 == rfile.openPerm {
		rfile.openPerm = 0660
	}
	rfile.file, err = os.OpenFile(rfile.Path, rfile.openFlag, rfile.openPerm)
	if nil != err {
		return err
	}
	st, serr := rfile.file.Stat()
	if nil != serr {
		rfile.close()
		return serr
	}
	rfile.fileSize = st.Size()
	if rfile.MaxBackupIndex <= 0 {
		rfile.MaxBackupIndex = 9
	}
	if rfile.MaxFileSize <= 0 {
		rfile.MaxFileSize = MaxRotateFileSize
	}
	if rfile.SyncBytesPeriod <= 0 {
		rfile.SyncBytesPeriod = 1024 * 1024
	}
	return nil
}

func (rfile *RotateFile) rotate() error {
	if nil == rfile.file {
		return nil
	}
	err := rfile.close()
	if nil != err {
		return err
	}
	for i := rfile.MaxBackupIndex - 1; i >= 0; i-- {
		old := fmt.Sprintf("%s.%d", rfile.Path, i+1)
		current := rfile.Path
		if i > 0 {
			current = fmt.Sprintf("%s.%d", rfile.Path, i)
		}
		os.Remove(old)
		os.Rename(current, old)
	}
	return nil
}

func (rfile *RotateFile) Write(p []byte) (int, error) {
	rfile.lk.Lock()
	defer rfile.lk.Unlock()
	if nil == rfile.file {
		err := rfile.reopen()
		if nil != err {
			return 0, err
		}
	}
	if nil == rfile.file {
		return 0, fmt.Errorf("File %s not open", rfile.Path)
	}
	n, err := rfile.file.Write(p)
	rfile.fileSize += int64(n)
	if rfile.fileSize >= rfile.MaxFileSize {
		err = rfile.rotate()
	} else if nil == err && rfile.fileSize%int64(rfile.SyncBytesPeriod) == 0 {
		err = rfile.file.Sync()
	}
	if nil != err {
		rfile.close()
	}
	return n, err
}
