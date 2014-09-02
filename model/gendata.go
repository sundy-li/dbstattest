package model

import (
    "bufio"
    "fmt"
    "os"
    "strings"
)

var file *bufio.Writer

// /data2/infinidb/data/bulk/data/import/trend_campaign.tbl

func insert(s string) (err error) {
    _, err = file.WriteString(s)
    if err != nil {
        return
    }
    file.WriteString("\n")
    if err != nil {
        return
    }
    return
}

func GenData(fileName string, rowCount int) {
    f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
    if err != nil {
        panic(err.Error())
    }
    file = bufio.NewWriter(f)
    for count := 0; count < rowCount; {
        count += insertCamps()
    }
}

func insertCamps() int {
    camps := GenCamps()
    var err error
    for _, c := range camps {
        rowStr := ""
        for _, f := range Fields {
            rowStr += fmt.Sprintf("%v|", c[f])
        }
        rowStr = strings.TrimSuffix(rowStr, "|")

        err = insert(rowStr)
        if err != nil {
            panic(err.Error())
        }
    }
    err = file.Flush()
    if err != nil {
        panic(err.Error())
    }
    return len(camps)
}
