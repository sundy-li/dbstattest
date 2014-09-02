package main

import (
    "dbstattest/model"
    "flag"
    "fmt"
)

var fileName string
var rowCount int

func init() {
    flag.StringVar(&fileName, "file", "", "文件名")
    flag.IntVar(&rowCount, "rc", 0, "行数")

}

func main() {
    flag.Parse()
    if fileName == "" {
        fmt.Println("file name is empty!")
        return
    }

    fmt.Println(fileName, " ", rowCount)
    model.GenData(fileName, rowCount)
}
