package model

import (
    "fmt"
)

const (
    es         = "es"
    mysql      = "mysql"
    infinidb   = "infinidb"
    infobright = "infobright"
)

const (
    Query1 = 1
    Query2 = 2
    Query3 = 3
)

func GetClientGen(dbType string) func() DbClient {
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

    return f
}

func TestQuery(client DbClient, testType int) {
    count := 1000
    count2 := 100
    var err error
    switch testType {
    case Query1:
        _, err = client.Query(GenCampIds(count))
    case Query2:
        date0, date1 := GenDateRange()
        _, err = client.Query2(GenCampIds(count), date0, date1)
    case Query3:
        _, err = client.Query3(GenCampIds(count), GenHours(count2))
    default:
        panic(fmt.Sprintf("wrong test type: %d\n", testType))
    }
    if err != nil {
        panic(err.Error())
    }
}
