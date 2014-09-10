package model

import (
    "fmt"
)

const (
    es         = "es"
    mysql      = "mysql"
    infinidb   = "infinidb"
    infobright = "infobright"
    memsql     = "memsql"
)

const (
    Query1 = 1
    Query2 = 2
    Query3 = 3
    Query4 = 4
    Query5 = 5
    Query6 = 6
    Query7 = 7
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
    case infinidb_single:
        f = func() DbClient {
            return NewInifiniDBSingleClient()
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

func TestQuery(client DbClient, testType int) (err error) {
    count := 1000
    count2 := 100
    switch testType {
    case Query1:
        _, err = client.Query(GenCampIds(count))
    case Query2:
        date0, date1 := GenDateRange()
        _, err = client.Query2(GenCampIds(count), date0, date1)
    case Query3:
        _, err = client.Query3(GenCampIds(count), GenHours(count2))
    case Query4:
        _, err = client.Query4(GenCampIds(count), GenCampIds(count), GenCampIds(count))
    case Query5:
        _, err = client.Query5(GenCampIds(count))
    case Query6:
        _, err = client.Query6(GenCampIds(count), GenCampIds(count), GenCampIds(count))
    case Query7:
        date0, date1 := GenDateRange()
        _, err = client.Query7(GenCampIds(count), date0, date1)
    default:
        panic(fmt.Sprintf("wrong test type: %d\n", testType))
    }
    return
}
