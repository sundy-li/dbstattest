package model

import (
    "fmt"
    "os/exec"
    . "sunteng/commons/db/mysql"
    "time"
)

var (
    InfoBrighthost     = "localhost"
    InfoBrightport     = 5029
    InfoBrightdatabase = "myib"
    InfoBrightusername = "root"
    InfoBrightpassword = "root"
)

type InfoBright struct {
    *MysqlClient
}

func (c *InfoBright) Init() {
    c.MysqlClient.db = ConnectMysqlDB(InfoBrighthost, InfoBrightport, InfoBrightdatabase, InfoBrightusername, InfoBrightpassword)
    if Debug {
        fmt.Printf("%v,%v,%v,%v,%v\n", InfoBrighthost, InfoBrightport, InfoBrightdatabase, InfoBrightusername, InfoBrightpassword)
    }
}

func NewInfoBrightClient() DbClient {
    return &InfoBright{
        MysqlClient: NewMysqlClient(),
    }
}

func (c *InfoBright) StartDB() error {
    cmd := exec.Command("/etc/init.d/mysqld-ib", "start")
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}

func (c *InfoBright) StopDB() error {
    cmd := exec.Command("/etc/init.d/mysqld-ib", "stop")
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}

func (c *InfoBright) LoadData(fileName string) error {
    sql := fmt.Sprintf("load data infile '%s' into table trend_campaign FIELDS TERMINATED by '|'", fileName)
    _, err := c.db.ExecErr(sql)
    return err
}
