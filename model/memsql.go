package model

import (
    "fmt"
    "os/exec"
    . "sunteng/commons/db/mysql"
    "time"
)

var (
    MemsqlDBhost     = "192.168.10.60"
    MemsqlDBport     = 3309
    MemsqlDBdatabase = "test"
    MemsqlDBusername = "root"
    MemsqlDBpassword = ""
)

type MemsqlDB struct {
    *MysqlClient
}

func (c *MemsqlDB) Init() {
    c.MysqlClient.db = ConnectMysqlDB(MemsqlDBhost, MemsqlDBport, MemsqlDBdatabase, MemsqlDBusername, MemsqlDBpassword)
    if Debug {
        fmt.Printf("%v,%v,%v,%v,%v\n", MemsqlDBhost, MemsqlDBport, MemsqlDBdatabase, MemsqlDBusername, MemsqlDBpassword)
    }
}

func NewMemsqlDBClient() DbClient {
    return &MemsqlDB{
        MysqlClient: NewMysqlClient(),
    }
}

func (c *MemsqlDB) StartDB() error {
    // cmd := exec.Command("/data1/memsqlbin/memsqld ", "-uroot", "--port=3309")
    // err := cmd.Run()
    // time.Sleep(time.Minute)
    // return err
    return nil
}

func (c *MemsqlDB) StopDB() error {
    cmd := exec.Command("pkill memsql")
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}

func (c *MemsqlDB) LoadData(fileName string) error {
    str := fmt.Sprintf("load data infile '%s' into table trend_campaign FIELDS TERMINATED by '|'", fileName)
    _, err := c.db.ExecErr(str)
    return err
}
