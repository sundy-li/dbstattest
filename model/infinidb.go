package model

import (
    "fmt"
    "os/exec"
    . "sunteng/commons/db/mysql"
    "time"
)

var (
    InfiniDBhost     = "192.168.10.72"
    InfiniDBport     = 3407
    InfiniDBdatabase = "myinfinidb"
    InfiniDBusername = "root"
    InfiniDBpassword = "root"
)

type InfiniDB struct {
    *MysqlClient
}

func (c *InfiniDB) Init() {
    c.MysqlClient.db = ConnectMysqlDB(InfiniDBhost, InfiniDBport, InfiniDBdatabase, InfiniDBusername, InfiniDBpassword)
    if Debug {
        fmt.Printf("%v,%v,%v,%v,%v\n", InfiniDBhost, InfiniDBport, InfiniDBdatabase, InfiniDBusername, InfiniDBpassword)
    }
}

func NewInifiniDBClient() DbClient {
    return &InfiniDB{
        MysqlClient: NewMysqlClient(),
    }
}

func (c *InfiniDB) StartDB() error {
    cmd := exec.Command("/usr/local/Calpont/bin/calpontConsole", "startSystem")
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}

func (c *InfiniDB) StopDB() error {
    cmd := exec.Command("/usr/local/Calpont/bin/calpontConsole", "shutdownSystem", "y")
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}

func (c *InfiniDB) LoadData(fileName string) error {
    cmd := exec.Command("/usr/local/Calpont/bin/cpimport", "-m1", InfiniDBdatabase, "trend_campaign", fileName)
    fmt.Println("/usr/local/Calpont/bin/cpimport", "-m1", InfiniDBdatabase, "trend_campaign", fileName)
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}
