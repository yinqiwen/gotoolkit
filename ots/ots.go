package ots

import (
	"bytes"
	//"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

type commandHandler struct {
	handler func([]string, io.Writer) error
	minArgs int
	maxArgs int
}

var commandHandlers map[string]*commandHandler = make(map[string]*commandHandler)
var usages []string

func help(cmd []string, c io.Writer) error {
	fmt.Fprintln(c, "Supported Commands:")
	for _, usage := range usages {
		fmt.Fprintln(c, usage)
	}
	fmt.Fprintln(c, "")
	return nil
}

var profile *os.File
var profLock sync.Mutex

func startCPUProfile(args []string, c io.Writer) error {
	profLock.Lock()
	defer profLock.Unlock()
	if nil != profile {
		return fmt.Errorf("Previous cpu profile task is not stoped.")
	}
	var profileName string
	if len(args) == 0 {
		profileName = fmt.Sprintf("./cpuprof.%s.%d", time.Now().Format("20060102150405"), os.Getpid())
	} else {
		profileName = args[0]
	}
	var err error
	profile, err = os.Create(profileName)
	if err == nil {
		err = pprof.StartCPUProfile(profile)
	}
	return err
}

func stopCPUProfile(cmd []string, c io.Writer) error {
	profLock.Lock()
	defer profLock.Unlock()
	if nil == profile {
		return fmt.Errorf("No running cpu profile task.")
	}
	pprof.StopCPUProfile()
	profile.Close()
	profile = nil
	return nil
}

func stackDump(args []string, c io.Writer) error {
	var dumpfileName string
	if len(args) == 0 {
		dumpfileName = fmt.Sprintf("./stackdump.%s.%d", time.Now().Format("20060102150405"), os.Getpid())
	} else {
		dumpfileName = args[0]
	}
	dumpfile, err := os.Create(dumpfileName)
	if err == nil {
		defer dumpfile.Close()
		stackBuf := make([]byte, 1024*1024)
		n := runtime.Stack(stackBuf, true)
		_, err = dumpfile.Write(stackBuf[0:n])
	}
	return err
}

func stat(args []string, c io.Writer) error {
	fmt.Fprintf(c, "GOVersion: %s\n", runtime.Version())
	fmt.Fprintf(c, "PID: %d\n", os.Getpid())
	fmt.Fprintf(c, "PPID: %d\n", os.Getppid())
	fmt.Fprintf(c, "NumCPU: %d\n", runtime.NumCPU())
	fmt.Fprintf(c, "NumCgoCall: %d\n", runtime.NumCgoCall())
	fmt.Fprintf(c, "NumGoroutine: %d\n", runtime.NumGoroutine())
	fmt.Fprintf(c, "GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))
	// var memstat runtime.MemStats
	// runtime.ReadMemStats(&memstat)
	// b, err := json.MarshalIndent(memstat, " ", "    ")
	// if nil == err {
	// 	fmt.Fprintf(c, "MemStats:\n")
	// 	c.Write(b)
	// }
	return nil
}

func gc(args []string, c io.Writer) error {
	runtime.GC()
	return nil
}

func quit(args []string, c io.Writer) error {
	return io.EOF
}

func ProcessTroubleShooting(rwc io.ReadWriteCloser) {
	data := make([]byte, 0)
	buf := make([]byte, 1024)
	for {
		n, err := rwc.Read(buf)
		if nil != err {
			break
		}
		if len(data) == 0 {
			data = buf[0:n]
		} else {
			data = append(data, buf[0:n]...)
		}
		if len(data) > 1024 {
			fmt.Fprintf(rwc, "Too long command from input.")
			break
		}
		i := bytes.IndexByte(data, '\n')
		if -1 == i {
			continue
		}
		line := strings.TrimSpace(string(data[0:i]))
		data = data[i+1:]
		if len(line) == 0 {
			continue
		}
		cmd := strings.Fields(line)
		if h, ok := commandHandlers[strings.ToLower(cmd[0])]; ok {
			args := cmd[1:]
			if (h.minArgs >= 0 && len(args) < h.minArgs) || (h.maxArgs >= 0 && len(args) > h.maxArgs) {
				fmt.Fprintf(rwc, "Invalid args in command:%s \r\n", line)
				continue
			}
			err := h.handler(args, rwc)
			if nil != err {
				if err == io.EOF {
					break
				}
				fmt.Fprintln(rwc, err)
			} else {
				fmt.Fprintf(rwc, "Execute '%s' success.\n", line)
			}
		} else {
			fmt.Fprintf(rwc, "Error:unknown command:%s\r\n", cmd)
			continue
		}
	}
	rwc.Close()
}

func StartTroubleShootingServer(laddr string) error {
	l, err := net.Listen("tcp", laddr)
	if nil != err {
		return err
	}

	go func() {
		for {
			c, _ := l.Accept()
			if nil != c {
				go ProcessTroubleShooting(c)
			}
		}
	}()
	return nil
}

func RegisterHandler(cmd string, handler func([]string, io.Writer) error, minArgs int, maxArgs int, usage string) error {
	cmdkey := strings.ToLower(cmd)
	if _, ok := commandHandlers[cmdkey]; ok {
		return fmt.Errorf("Duplicate command key:%s", cmdkey)
	}
	commandHandlers[cmdkey] = &commandHandler{handler, minArgs, maxArgs}
	if len(usage) > 0 {
		usages = append(usages, usage)
	}
	return nil
}

func init() {
	RegisterHandler("startCPUprofile", startCPUProfile, 0, 1, "StartCPUProfile  [File Path]       StartCPUProfile with given file path, default is ./cpu.prof.<time>.<pid>")
	RegisterHandler("stopCPUProfile", stopCPUProfile, 0, 0, "StopCPUProfile                     StopCPUProfile")
	RegisterHandler("stackDump", stackDump, 0, 1, "StackDump  [File Path]             Dump all goroutine statck trace, default is ./stackdump.<time>.<pid>")
	RegisterHandler("stat", stat, 0, 0, "Stat                               Print runtime stat")
	RegisterHandler("gc", gc, 0, 0, "GC                                 Call GC")
	RegisterHandler("exit", quit, 0, 0, "Exit                               exit current session")
	RegisterHandler("quit", quit, 0, 0, "")
	RegisterHandler("help", help, 0, 0, "")
}
