package main

import (
    . "dbstattest/model"
    "flag"
    "fmt"
    "runtime"
    "sort"
    "sync"
    "time"

    // "data_center/sandwich/lib/util"
)

var queryLimit int
var goNumber int

var debug bool
var queryCount = 0
var quickChan = make(chan bool, 1)

var intervals = []int64{10, 20, 30, 40, 50, 60, 70, 80, 90}
var space int64 = 100
var times = map[string]int{}
var lock sync.Mutex

var testType = Query2

var dbType string

type Record struct {
    Name  string
    Count int
    Rate  string
}

type Records []*Record

func (p Records) Len() int           { return len(p) }
func (p Records) Less(i, j int) bool { return p[i].Count > p[j].Count }
func (p Records) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func init() {
    flag.IntVar(&queryLimit, "limit", 10, "查询数量")
    flag.IntVar(&goNumber, "go", 5, "go程数量")
    flag.IntVar(&testType, "type", Query1, "测试类型")
    flag.StringVar(&dbType, "db", "mysql", "数据库类型")
    flag.BoolVar(&debug, "debug", false, "是否debug")
}

func init() {
    runtime.GOMAXPROCS(runtime.NumCPU() - 1)

    oldLen := len(intervals)
    intervals = append(intervals, make([]int64, 200)...)
    base := space
    for i := oldLen; i < len(intervals); i++ {
        intervals[i] = base
        if i >= 50 {
            space = 500
        } else if i >= 100 {
            space = 1000
        }
        base += space
    }

    // fmt.Printf("intervals: %v\n", intervals)
}

func main() {
    flag.Parse()
    Debug = debug

    test(dbType)

}

func test(dbType string) {
    gentor := GetClientGen(dbType)
    times = map[string]int{}
    queryCount = 0

    now := time.Now()
    for i := 0; i < goNumber; i++ {
        go query(gentor())
    }

    for i := 0; i < goNumber; i++ {
        <-quickChan
    }

    totalSpend := time.Now().Sub(now).Seconds()

    // util.PrintInJSON(times, "spend times")

    records := make([]*Record, len(times))
    i := 0
    total := 0
    for dbType, count := range times {
        records[i] = &Record{
            Name:  dbType,
            Count: count,
        }
        i++
        total += count
    }
    sort.Sort(Records(records))
    fmt.Printf("=========\ntime spend of %v, type:%d, goroutine: %d,  total query count: %d, total spend time: %.4fs, qps: %.4f\n", dbType, testType, goNumber, total, totalSpend, float64(total)/totalSpend)
    for _, r := range records {
        r.Rate = fmt.Sprintf("%.4f%%", float32(r.Count)/float32(total)*100)
        fmt.Printf("%s:\t%d,\t%s\n", r.Name, r.Count, r.Rate)
    }

}

func query(client DbClient) {
    client.Init()
    for {
        if queryCount >= queryLimit {
            break
        }

        now := time.Now()
        TestQuery(client, testType)
        handleTime(now)

        queryCount++
        if queryCount >= queryLimit {
            break
        }
    }
    quickChan <- true
}

func handleTime(last time.Time) {
    lock.Lock()
    defer lock.Unlock()

    ms := time.Now().Sub(last).Nanoseconds() / 1000000
    p := -1
    i := 0
    itLeng := len(intervals)
    for ; i < itLeng; i++ {
        if i == 0 && ms <= intervals[i] {
            p = -1
        } else if i == (itLeng - 1) {
            p = i
        } else if ms > intervals[i] && ms <= intervals[i+1] {
            p = i
            break
        }
    }
    s := ""
    if p == -1 {
        s = fmt.Sprintf("0 ~ %vms", intervals[0])
    } else if p == itLeng-1 {
        s = fmt.Sprintf("%vms ~ unknown", intervals[p])
    } else {
        s = fmt.Sprintf("%vms ~ %vms", intervals[p], intervals[p+1])
    }
    count, _ := times[s]
    count++
    times[s] = count
}
