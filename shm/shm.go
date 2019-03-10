// +build darwin dragonfly freebsd linux netbsd openbsd

// Package shm implements System V shared memory functions (shmctl, shmget, shmat, shmdt).
package shm

import (
	"syscall"
	"unsafe"
)

// Constants.
const (
	// Mode bits for `shmget`.

	// Create key if key does not exist.
	IPC_CREAT = 01000
	// Fail if key exists.
	IPC_EXCL = 02000
	// Return error on wait.
	IPC_NOWAIT = 04000

	// Special key values.

	// Private key.
	IPC_PRIVATE = 0

	// Flags for `shmat`.

	// Attach read-only access.
	SHM_RDONLY = 010000
	// Round attach address to SHMLBA.
	SHM_RND = 020000
	// Take-over region on attach.
	SHM_REMAP = 040000
	// Execution access.
	SHM_EXEC = 0100000

	// Commands for `shmctl`.

	// Lock segment (root only).
	SHM_LOCK = 1
	// Unlock segment (root only).
	SHM_UNLOCK = 12

	// Control commands for `shmctl`.

	// Remove identifier.
	IPC_RMID = 0
	// Set `ipc_perm` options.
	IPC_SET = 1
	// Get `ipc_perm' options.
	IPC_STAT = 2
)

// Ftok returns a probably-unique key that can be used by System V IPC
// syscalls, e.g. msgget().
// See ftok(3) and https://code.woboq.org/userspace/glibc/sysvipc/ftok.c.html
func ftok(path string, id uint8) (uint64, error) {
	st := &syscall.Stat_t{}
	if err := syscall.Stat(path, st); err != nil {
		return 0, err
	}
	return uint64((st.Ino & 0xffff) | uint64((st.Dev&0xff)<<16) | uint64((id&0xff)<<24)), nil
}

// Get allocates a shared memory segment.
//
// Get() returns the identifier of the shared memory segment associated with the value of the argument key.
// A new shared memory segment is created if key has the value IPC_PRIVATE or key isn't IPC_PRIVATE,
// no shared memory segment corresponding to key exists, and IPC_CREAT is specified in shmFlg.
//
// If shmFlg specifies both IPC_CREAT and IPC_EXCL and a shared memory segment already exists for key,
// then Get() fails with errno set to EEXIST.
func shmGet(key uint64, size uint64, shmFlg int) (shmId int, err error) {
	id, _, errno := syscall.Syscall(syscall.SYS_SHMGET, uintptr(int32(key)), uintptr(size), uintptr(int32(shmFlg)))
	if int(id) == -1 {
		return -1, errno
	}
	return int(id), nil
}

// At attaches the shared memory segment identified by shmId.
//
// Using At() with shmAddr equal to NULL is the preferred, portable way of attaching a shared memory segment.
func shmAttach(shmId int, shmAddr uintptr, shmFlg int) (data []byte, err error) {
	addr, _, errno := syscall.Syscall(syscall.SYS_SHMAT, uintptr(int32(shmId)), shmAddr, uintptr(int32(shmFlg)))
	if int(addr) == -1 {
		return nil, errno
	}
	length, err := shmGetSize(shmId)
	if err != nil {
		return nil, err
	}

	var b = struct {
		addr uintptr
		len  int
		cap  int
	}{addr, int(length), int(length)}
	data = *(*[]byte)(unsafe.Pointer(&b))
	return data, nil
}

// Dt detaches the shared memory segment.
//
// The to-be-detached segment must be currently attached with shmAddr equal to the value returned by the attaching At() call.
func shmDettach(data []byte) error {
	result, _, errno := syscall.Syscall(syscall.SYS_SHMDT, uintptr(unsafe.Pointer(&data[0])), 0, 0)
	if int(result) == -1 {
		return errno
	}

	return nil
}

// Ctl performs the control operation specified by cmd on the shared memory segment whose identifier is given in shmId.
//
// The buf argument is a pointer to a IdDs structure.
func shmCtl(shmId int, cmd int, buf *IdDs) (int, error) {
	result, _, errno := syscall.Syscall(syscall.SYS_SHMCTL, uintptr(int32(shmId)), uintptr(int32(cmd)), uintptr(unsafe.Pointer(buf)))
	if int(result) == -1 {
		return -1, errno
	}

	return int(result), nil
}

// Rm removes the shared memory segment.
func shmRemove(shmId int) error {
	_, err := shmCtl(shmId, IPC_RMID, nil)
	return err
}

// Size returns size of shared memory segment.
func shmGetSize(shmId int) (uint64, error) {
	var idDs IdDs
	_, err := shmCtl(shmId, IPC_STAT, &idDs)
	if err != nil {
		return 0, err
	}

	return idDs.SegSz, nil
}
