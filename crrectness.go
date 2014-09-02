package main

import (
    . "dbstattest/model"
    "flag"
    "fmt"
    "runtime"
    "strings"
    "sync"
    "time"

    // "data_center/sandwich/lib/util"
)

var quickChan = make(chan bool, 1)
var lock sync.Mutex

const (
    Query1 = 1
    Query2 = 2
    Query3 = 3
)

var testTypes = []int{Query1, Query2, Query3}
var queryLimit int
var goNumber int
var debug bool

var dbTypes string

const (
    es         = "es"
    mysql      = "mysql"
    infinidb   = "infinidb"
    infobright = "infobright"
)

var queryCount = 0
var totalQueryTime float64 = 0

func init() {
    flag.IntVar(&queryLimit, "limit", 10, "查询数量")
    flag.IntVar(&goNumber, "go", 5, "go程数量")
    flag.StringVar(&dbTypes, "db", mysql, "数据库类型, 如'mysql,es,infinidb,infobright'")
    flag.BoolVar(&debug, "debug", false, "是否debug")
}

func init() {
    runtime.GOMAXPROCS(runtime.NumCPU() - 1)
}

func main() {
    flag.Parse()
    Debug = debug

    for _, dbType := range strings.Split(dbTypes, ",") {
        queryDb(dbType)
    }
}

func queryDb(dbType string) {
    var f func() DbClient

    switch dbType {
    case es:
        f = func() DbClient {
            return NewESClient()
        }
    case mysql:
        f = func() DbClient {
            return NewMysqlClient()
        }
    case infinidb:
        f = func() DbClient {
            return NewInifiniDBClient()
        }
    case infobright:
        f = func() DbClient {
            return NewInfoBrightClient()
        }
    default:
        fmt.Printf("unknown dbType: %s", dbType)
    }

    client := f()
    client.Init()
    dataCount := client.DataCount()
    client.Destroy()

    for _, testType := range testTypes {
        queryType(f, dbType, testType)
    }
}

func reset() {
    queryCount = 0
    totalQueryTime = 0
}

func queryType(f func() DbClient, name string, testType int) {
    reset()

    now := time.Now()
    for i := 0; i < goNumber; i++ {
        go query(i, f(), testType)
    }

    spend := time.Now().Sub(now)
    totalSpend := ""
    if spend.Seconds() > 60 {
        totalSpend = fmt.Sprintf("%.3fm", spend.Minutes())
    } else {
        totalSpend = fmt.Sprintf("%.3fs", spend.Seconds())
    }

    fmt.Printf("Q%d\t%s\t%.4fs\t%.4f\n", testType, totalSpend, totalQueryTime/float64(queryCount), float64(queryCount)/spend.Seconds())
}

func query(gonum int, client DbClient, testType int) (rows interface{}) {
    client.Init()
    var err error

    count := 1000
    count2 := 100
    now := time.Now()
    switch testType {
    case Query1:
        rows, err = client.Query(GenCampIds(count))
    case Query2:
        date0, date1 := GenDateRange()
        rows, err = client.Query2(GenCampIds(count), date0, date1)
    case Query3:
        rows, err = client.Query3(GenCampIds(count), GenHours(count2))
    default:
        panic(fmt.Sprintf("wrong test type: %d\n", testType))
    }
    if err != nil {
        panic(err.Error())
    }
}

func incCount() bool {
    lock.Lock()
    if queryCount >= queryLimit {
        lock.Unlock()
        return false
    }
    // 加查询总数
    queryCount++
    lock.Unlock()
    return true
}
