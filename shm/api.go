package shm

import (
	"fmt"
	"os"
)

type Segment struct {
	Data  []byte
	shmID int
	key   uint64
}

func (seg *Segment) Close() error {
	if len(seg.Data) == 0 {
		return nil
	}
	err := shmDettach(seg.Data)
	if nil == err {
		seg.Data = make([]byte, 0)
	}
	return err
}

func (seg *Segment) Remove() error {
	if 0 == seg.shmID {
		return nil
	}
	err := shmRemove(seg.shmID)
	if nil == err {
		seg.shmID = 0
	}
	return err
}

type InitOptions struct {
	Path      string
	ProjId    uint8
	Readonly  bool
	Exclusive bool
	MaxSize   uint64
}

func Open(opt *InitOptions) (*Segment, error) {
	os.Create(opt.Path)
	shmKey, err := ftok(opt.Path, opt.ProjId)
	if nil != err {
		return nil, err
	}
	existShmId, err := shmGet(shmKey, opt.MaxSize, 0666)
	if opt.Readonly {
		if nil != err {
			return nil, err
		}
		data, err := shmAttach(existShmId, 0, SHM_RDONLY)
		if nil != err {
			return nil, err
		}
		seg := &Segment{
			Data:  data,
			shmID: existShmId,
			key:   shmKey,
		}
		return seg, nil
	}

	rwShmId := existShmId
	shmCreate := false
	if opt.Exclusive {
		if nil == err {
			return nil, fmt.Errorf("exist shm with path:%s", opt.Path)
		}
		shmId, err := shmGet(shmKey, opt.MaxSize, IPC_CREAT|IPC_EXCL|0666)
		if nil != err {
			return nil, err
		}
		shmCreate = true
		rwShmId = shmId
	} else {
		if nil != err {
			shmId, err := shmGet(shmKey, opt.MaxSize, IPC_CREAT|0666)
			if nil != err {
				return nil, err
			}
			shmCreate = true
			rwShmId = shmId
		} else {
			existSize, err := shmGetSize(existShmId)
			if nil != err {
				return nil, err
			}
			if existSize != opt.MaxSize {
				return nil, fmt.Errorf("expected exist shm size:%d while current size:%d", opt.MaxSize, existSize)
			}
		}
	}

	data, err := shmAttach(rwShmId, 0, 0)
	if nil != err {
		if shmCreate {
			rmErr := shmRemove(rwShmId)
			if nil != rmErr {
				return nil, rmErr
			}
		}
		return nil, err
	}
	seg := &Segment{
		Data:  data,
		shmID: existShmId,
		key:   shmKey,
	}
	return seg, nil
}
