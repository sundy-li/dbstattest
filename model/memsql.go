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
    MemsqlDBdatabase = "dsp"
    MemsqlDBusername = "root"
    MemsqlDBpassword = ""
)

var memsql_dataCountSql = "SELECT count(*) from trend_campaign"

var memsql_querySql1 = "SELECT sql_big_result %s, campaign_id FROM trend_campaign WHERE campaign_id IN (%s) GROUP BY campaign_id LIMIT 20 OFFSET 0"

// 全指标-IN+Range条件-Limit
var memsql_querySql2 = "SELECT sql_big_result %s,campaign_id FROM trend_campaign WHERE campaign_id IN (%s) AND date >= %d AND date <= %d GROUP BY campaign_id LIMIT 20 OFFSET 0"

// 全指标-IN+IN条件-汇总结果Range条件-Order-Limit+Offset
var memsql_querySql3 = "SELECT sql_big_result %s,campaign_id FROM trend_campaign WHERE campaign_id IN (%s) AND hour IN (%s) GROUP BY campaign_id, hour HAVING SUM(cost) > 100  ORDER BY SUM(impressions) desc LIMIT 20 OFFSET 100"

// 全指标-多维度-3个IN条件-GroupBy四个维度-Limit+Offset
var memsql_querySql4 = "SELECT sql_big_result %s, campaign_id, product_id, spot_id, whisky_id FROM trend_campaign WHERE campaign_id IN (%s) AND product_id IN(%s) AND spot_id IN (%s) GROUP BY campaign_id, product_id, spot_id, whisky_id LIMIT 50 OFFSET 400"

// 三个指标-Group by三个维度
var memsql_querySql5 = "SELECT sql_big_result campaign_id, date, hour, sum(ips), sum(impressions), sum(new_impressions) FROM trend_campaign WHERE campaign_id IN(%s) GROUP BY campaign_id, date, hour LIMIT 50 OFFSET 300"

// 汇总数据
var memsql_querySql6 = "SELECT sql_big_result %s FROM trend_campaign WHERE campaign_id IN (%s) AND product_id IN(%s) AND spot_id IN (%s) GROUP BY campaign_id, product_id, spot_id"

// 数量
var memsql_querySql7 = "SELECT  sql_big_result count(*) FROM trend_campaign WHERE campaign_id IN (%s) AND date >= %d AND date <= %d"

type MemsqlDB struct {
    *MysqlClient
}

func (c *MemsqlDB) Init() {
    c.MysqlClient.db = ConnectMysqlDB(MemsqlDBhost, MemsqlDBport, MemsqlDBdatabase, MemsqlDBusername, MemsqlDBpassword)
    if Debug {
        fmt.Printf("%v,%v,%v,%v,%v\n", MemsqlDBhost, MemsqlDBport, MemsqlDBdatabase, MemsqlDBusername, MemsqlDBpassword)
    }
    c.dataCountSql = memsql_dataCountSql
    c.querySql1 = memsql_querySql1
    c.querySql2 = memsql_querySql2
    c.querySql3 = memsql_querySql3
    c.querySql4 = memsql_querySql4
    c.querySql5 = memsql_querySql5
    c.querySql6 = memsql_querySql6
    c.querySql7 = memsql_querySql7
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
    cmd := exec.Command(`killall memsql && ssh root@192.168.10.71 "killall memsql" && ssh root@192.168.10.72 "killall memsql"`)
    err := cmd.Run()
    time.Sleep(time.Minute)
    return err
}

func (c *MemsqlDB) LoadData(fileName string) error {
    str := fmt.Sprintf(`load data infile '%s' into table trend_campaign  FIELDS TERMINATED by '|'`, fileName)
    _, err := c.db.ExecErr(str)
    return err
}
