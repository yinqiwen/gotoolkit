package ots

import (
	"bytes"
	"path/filepath"
	//"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
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

var profLock sync.Mutex

func cpuProfile(args []string, c io.Writer) error {
	profLock.Lock()
	defer profLock.Unlock()
	sleepSecs := time.Duration(10)
	if len(args) > 0 {
		secs, serr := strconv.Atoi(args[0])
		if nil != serr {
			return serr
		}
		sleepSecs = time.Duration(secs)
	}
	var profileName string
	if len(args) < 2 {
		profileName = fmt.Sprintf("./%s.cpuprof.%s.%d", filepath.Base(os.Args[0]), time.Now().Format("20060102150405"), os.Getpid())
	} else {
		profileName = args[1]
	}
	profile, err := os.Create(profileName)
	if err == nil {
		err = pprof.StartCPUProfile(profile)
		defer profile.Close()
	}
	if nil == err {
		fmt.Fprintf(c, "Wait %d seconds to collect cpu profile info...\n", sleepSecs)
		time.Sleep(sleepSecs * time.Second)
		pprof.StopCPUProfile()
	}
	return err
}

func stackDump(args []string, c io.Writer) error {

	if len(args) == 0 {
		//dumpfileName = fmt.Sprintf("./%s.stackdump.%s.%d", filepath.Base(os.Args[0]), time.Now().Format("20060102150405"), os.Getpid())
	} else {
		var dumpfileName string
		dumpfileName = args[0]
		dumpfile, err := os.Create(dumpfileName)
		if nil != err {
			return err
		}
		defer dumpfile.Close()
		c = dumpfile
	}
	stackBuf := make([]byte, 1024*1024*64)
	n := runtime.Stack(stackBuf, true)
	_, err := c.Write(stackBuf[0:n])
	return err
}

func memprof(args []string, c io.Writer) error {
	var dumpfileName string
	if len(args) == 0 {
		dumpfileName = fmt.Sprintf("./%s.memprof.%s.%d", filepath.Base(os.Args[0]), time.Now().Format("20060102150405"), os.Getpid())
	} else {
		dumpfileName = args[0]
	}
	dumpfile, err := os.Create(dumpfileName)
	if err == nil {
		defer dumpfile.Close()
		err = pprof.WriteHeapProfile(dumpfile)
	}
	return err
}

func blockProfile(args []string, c io.Writer) error {
	secs, serr := strconv.Atoi(args[0])
	if nil != serr {
		return serr
	}

	var dumpfileName string
	if len(args) == 1 {
		dumpfileName = fmt.Sprintf("./%s.blockprof.%s.%d", filepath.Base(os.Args[0]), time.Now().Format("20060102150405"), os.Getpid())
	} else {
		dumpfileName = args[1]
	}
	runtime.SetBlockProfileRate(1)
	defer runtime.SetBlockProfileRate(0)
	fmt.Fprintf(c, "Wait %d seconds to collect block profile info...\n", secs)
	time.Sleep(time.Duration(secs) * time.Second)
	dumpfile, err := os.Create(dumpfileName)
	if err == nil {
		defer dumpfile.Close()
		err = pprof.Lookup("block").WriteTo(dumpfile, 1)
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
	fmt.Fprintf(c, "GODEBUG: %s\n", os.Getenv("GODEBUG"))

	var memstat runtime.MemStats
	runtime.ReadMemStats(&memstat)
	fmt.Fprintf(c, "HeapIdle: %d\n", memstat.HeapIdle)
	fmt.Fprintf(c, "HeapInuse: %d\n", memstat.HeapInuse)
	fmt.Fprintf(c, "HeapObjects: %d\n", memstat.HeapObjects)
	return nil
}

func gc(args []string, c io.Writer) error {
	runtime.GC()
	debug.FreeOSMemory()
	return nil
}

func quit(args []string, c io.Writer) error {
	return io.EOF
}

func Handle(line string, wr io.Writer) error {
	cmd := strings.Fields(line)
	if h, ok := commandHandlers[strings.ToLower(cmd[0])]; ok {
		args := cmd[1:]
		if (h.minArgs >= 0 && len(args) < h.minArgs) || (h.maxArgs >= 0 && len(args) > h.maxArgs) {
			fmt.Fprintf(wr, "Invalid args in command:%s \r\n", line)
			return nil
		}
		err := h.handler(args, wr)
		if nil != err {
			if err != io.EOF {
				fmt.Fprintln(wr, err)
			}
			return err
		}
		fmt.Fprintf(wr, "Execute '%s' success.\n", line)
	} else {
		fmt.Fprintf(wr, "Error:unknown command:%s\r\n", cmd[0])
	}
	return nil
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

		err = Handle(line, rwc)
		if err == io.EOF {
			break
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
	RegisterHandler("CPUprofile", cpuProfile, 0, 2, "CPUProfile [Seconds] [File Path]   CPUProfile with given time & file path, default is 10 ./cpu.prof.<time>.<pid>")
	RegisterHandler("stackDump", stackDump, 0, 1, "StackDump  [File Path]             Dump all goroutine statck trace, default is ./stackdump.<time>.<pid>")
	RegisterHandler("MemProfile", memprof, 0, 1, "MemProfile [File Path]             Dump heap profile info, default is ./heaprof.<time>.<pid>")
	RegisterHandler("blockProfile", blockProfile, 1, 2, "BlockProfile <Seconds> [File Path] Dump block profile info, default is ./blockprof.<time>.<pid>")
	RegisterHandler("stat", stat, 0, 0, "Stat                               Print runtime stat")
	RegisterHandler("gc", gc, 0, 0, "GC                                 Call GC")
	RegisterHandler("exit", quit, 0, 0, "Exit                               exit current session")
	RegisterHandler("quit", quit, 0, 0, "")
	RegisterHandler("help", help, 0, 0, "")
}
