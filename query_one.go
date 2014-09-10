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

var testTypes = []int{Query1, Query2, Query3, Query4, Query5, Query6, Query7}
var queryLimit int
var goNumber int
var debug bool

var dataFiles string

const (
    dataRoot = "/data2/infinidb/data/bulk/data/import/"
)

var dbTypes string
var queryCount = 0
var totalQueryTime float64 = 0
var boot = false

func init() {
    flag.IntVar(&queryLimit, "limit", 10, "查询数量")
    flag.IntVar(&goNumber, "go", 5, "go程数量")
    flag.StringVar(&dbTypes, "db", "mysql", "数据库类型, 如'mysql,es,infinidb,infobright'")
    flag.StringVar(&dataFiles, "data", "", "测试所有查询之前插入的数据文件，如 '1kw.csv,4kw.csv', 根目录为/data2/infinidb/data/bulk/data/import/")
    flag.BoolVar(&boot, "boot", false, "是否要查询之前启动，查询之后关闭db")
    flag.BoolVar(&debug, "debug", false, "是否debug")
}

func init() {
    runtime.GOMAXPROCS(runtime.NumCPU() - 1)
}

func main() {
    flag.Parse()
    Debug = debug

    var err error

    for _, dbType := range strings.Split(dbTypes, ",") {
        f := GetClientGen(dbType)

        client := f()
        client.Init()

        if boot {
            err = client.StartDB()
            if err != nil {
                fmt.Printf("StartDB %s error: %s", dbType, err.Error())
                continue
            }
        }

        if dataFiles == "" {
            queryDb(dbType, f, client)
        } else {
            fs := strings.Split(dataFiles, ",")
            for _, file := range fs {
                realFile := ""
                if strings.HasPrefix(file, "/") {
                    realFile = file
                } else {
                    realFile = dataRoot + file
                }
                fmt.Printf("Loading data[%s] into %s\n", realFile, dbType)
                now := time.Now()
                err = client.LoadData(realFile)
                if err != nil {
                    fmt.Printf("LoadData %s error: %s", dbType, err.Error())
                    break
                }
                spendStr, _ := calTimeSpend(now)
                fmt.Printf("Load data time spend: %s\n", spendStr)

                queryDb(dbType, f, client)
            }
        }

        if boot {
            err = client.StopDB()
            if err != nil {
                fmt.Printf("StopDB %s error: %s", dbType, err.Error())
            }
        }
        client.Destroy()
    }
}

func queryDb(dbType string, f func() DbClient, initClient DbClient) {
    dataCount := initClient.DataCount()
    fmt.Printf("==================%s, dataCount: %d, goroutine: %d, count: %d\n", dbType, dataCount, goNumber, queryLimit)
    fmt.Printf("QType\ttotalTime\tavg\tqps\n")

    for _, testType := range testTypes {
        queryType(f, testType)
    }
}

func reset() {
    queryCount = 0
    totalQueryTime = 0
}

func queryType(f func() DbClient, testType int) {
    reset()

    now := time.Now()
    for i := 0; i < goNumber; i++ {
        go query(f(), testType)
    }

    for i := 0; i < goNumber; i++ {
        <-quickChan
    }

    spendStr, duration := calTimeSpend(now)
    fmt.Printf("Q%d\t%s\t%.4fs\t%.4f\n", testType, spendStr, totalQueryTime/float64(queryCount), float64(queryCount)/duration.Seconds())
}

func query(client DbClient, testType int) {
    client.Init()
    for {
        if !incCount() {
            break
        }
        now := time.Now()
        err := TestQuery(client, testType)
        if err != nil {
            fmt.Printf("Query %d error: %s", testType, err.Error())
            break
        }
        addTime(now)
    }
    client.Destroy()
    quickChan <- true
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

func addTime(last time.Time) {
    spend := time.Now().Sub(last)
    lock.Lock()
    totalQueryTime += spend.Seconds()
    lock.Unlock()
}

func calTimeSpend(last time.Time) (spend string, sub time.Duration) {
    sub = time.Now().Sub(last)
    if sub.Seconds() > 60 {
        spend = fmt.Sprintf("%.3fm", sub.Minutes())
    } else {
        spend = fmt.Sprintf("%.3fs", sub.Seconds())
    }
    return
}
