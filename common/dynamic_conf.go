package common

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	ConfCheckSnapTimeout      = "check_snap_timeout"
	ConfCheckRaftTimeout      = "check_raft_timeout"
	ConfIgnoreStartupNoBackup = "ignore_startup_nobackup"
	ConfIgnoreRemoteFileSync  = "ignore_remote_file_sync"
	ConfMaxRemoteRecover      = "max_remote_recover"
)

var intConfMap map[string]*int64
var strConfMap sync.Map

func init() {
	intConfMap = make(map[string]*int64)
	snapCheckTimeout := int64(60)
	intConfMap[ConfCheckSnapTimeout] = &snapCheckTimeout
	raftCheckTimeout := int64(5)
	intConfMap[ConfCheckRaftTimeout] = &raftCheckTimeout
	emptyInt := int64(0)
	intConfMap["empty_int"] = &emptyInt
	maxRemoteRecover := int64(2)
	intConfMap[ConfMaxRemoteRecover] = &maxRemoteRecover

	strConfMap.Store("test_str", "test_str")
}

func DumpDynamicConf() []string {
	cfs := make([]string, 0, len(intConfMap)*2)
	for k, v := range intConfMap {
		iv := atomic.LoadInt64(v)
		cfs = append(cfs, k+":"+strconv.Itoa(int(iv)))
	}
	strConfMap.Range(func(k, v interface{}) bool {
		cfs = append(cfs, fmt.Sprintf("%v:%v", k, v))
		return true
	})
	sort.Sort(sort.StringSlice(cfs))
	return cfs
}

func SetIntDynamicConf(k string, newV int) {
	v, ok := intConfMap[k]
	if ok {
		atomic.StoreInt64(v, int64(newV))
	}
}

func IsConfSetted(k string) bool {
	iv := GetIntDynamicConf(k)
	if iv != 0 {
		return true
	}
	sv := GetStrDynamicConf(k)
	if sv != "" {
		return true
	}
	return false
}

func GetIntDynamicConf(k string) int {
	v, ok := intConfMap[k]
	if ok {
		return int(atomic.LoadInt64(v))
	}
	return 0
}

func SetStrDynamicConf(k string, newV string) {
	strConfMap.Store(k, newV)
}

func GetStrDynamicConf(k string) string {
	v, ok := strConfMap.Load(k)
	if !ok {
		return ""
	}
	return v.(string)
}
