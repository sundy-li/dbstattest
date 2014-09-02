package main

import (
    "data_center/sandwich/lib/util"
    . "dbstattest/model"

    "log"
    "time"
)

var insertLimit = 100000 //

func main() {
    log.SetFlags(log.Ltime | log.Lshortfile)

    insert(NewMysqlClient(), "mysql")
    // time.Sleep(time.Second * 10)
    // insert(NewESClient(), "es")
}

func insert(client DbClient, name string) {
    client.Init()
    now := time.Now()
    for i := 0; i < insertLimit; {
        camps := GenCamps()
        client.InsertBatch(camps)
        i += len(camps)
    }
    spend := util.CalSpendTime(now)
    log.Printf("%s, time: %v\n", name, spend)
}
