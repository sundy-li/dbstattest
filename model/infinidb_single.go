package model

import (
    "fmt"
    "os/exec"
    . "sunteng/commons/db/mysql"
    "time"
)

var (
    InfiniDBSinglehost     = "localhost"
    InfiniDBSingleport     = 3407
    InfiniDBSingledatabase = "myinfinidb"
    InfiniDBSingleusername = "root"
    InfiniDBSinglepassword = "root"
)

type InfiniDBSingle struct {
    *MysqlClient
}

func (c *InfiniDBSingle) Init() {
    c.MysqlClient.db = ConnectMysqlDB(InfiniDBSinglehost, InfiniDBSingleport, InfiniDBSingledatabase, InfiniDBSingleusername, InfiniDBSinglepassword)
    if Debug {
        fmt.Printf("%v,%v,%v,%v,%v\n", InfiniDBSinglehost, InfiniDBSingleport, InfiniDBSingledatabase, InfiniDBSingleusername, InfiniDBSinglepassword)
    }
}

func NewInifiniDBSingleClient() DbClient {
    return &InfiniDBSingle{
        MysqlClient: NewMysqlClient(),
    }
}

func (c *InfiniDBSingle) StartDB() error {
    cmd := exec.Command("/usr/local/Calpont/bin/calpontConsole", "startSystem")
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}

func (c *InfiniDBSingle) StopDB() error {
    cmd := exec.Command("/usr/local/Calpont/bin/calpontConsole", "shutdownSystem", "y")
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}

func (c *InfiniDBSingle) LoadData(fileName string) error {
    cmd := exec.Command("/usr/local/Calpont/bin/cpimport", "-m3", InfiniDBSingledatabase, "trend_campaign", fileName)
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}
