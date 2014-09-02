package model

import (
    "fmt"
    "os/exec"
    "strconv"
    "strings"
    . "sunteng/commons/db/mysql"
)

var (
    Mysqlhost     = "localhost"
    Mysqlport     = 3306
    Mysqldatabase = "estest"
    Mysqlusername = "root"
    Mysqlpassword = "root"
)

type MysqlClient struct {
    db DBWrapper
}

func (c *MysqlClient) Init() {
    c.db = ConnectMysqlDB(Mysqlhost, Mysqlport, Mysqldatabase, Mysqlusername, Mysqlpassword)
    if Debug {
        fmt.Printf("%v,%v,%v,%v,%v\n", Mysqlhost, Mysqlport, Mysqldatabase, Mysqlusername, Mysqlpassword)
    }
}

func (c *MysqlClient) Destroy() {
    c.db.DB.Close()
}

func (c *MysqlClient) StartDB() error {
    // panic("unsupported")
    return nil
}

func (c *MysqlClient) StopDB() error {
    // panic("unsupported")
    return nil
}

func (c *MysqlClient) LoadData(fileName string) error {
    sql := fmt.Sprintf("\"load data infile '%s' into table trend_campaign FIELDS TERMINATED by '|'\"", fileName)
    cmd := exec.Command("/usr/bin/mysql-ib", Mysqldatabase, "-proot", "-e", sql)
    return cmd.Run()
}

func (c *MysqlClient) InsertBatch(camps []Campaign) (count int64, err error) {
    res, err := c.db.ExecErr(genInsertSql(camps))
    if err != nil {
        return
    }
    return res.RowsAffected()
}

func (c *MysqlClient) Insert(camp Campaign) error {
    _, err := c.db.ExecErr(genInsertSql([]Campaign{camp}))
    return err
}

func NewMysqlClient() *MysqlClient {
    return &MysqlClient{}
}

var selectFields = []string{
    "ips",
    "impressions",
    "new_impressions",
    "visitors",
    "new_visitors",
    "reviews",
    "insights",
    "cost",
    "cost_over",
    "cost_over2",
    "pagepixels",
    "clicks",
    "spot_screen_id",
    "spot_size_id",
    "b_ips",
    "b_pageviews",
    "b_sessions",
    "b_visitors",
    "b_new_visitors",
    "b_bounces",
    "b_pagepixels",
    "b_staytime",
    "b_loadtime",
    "b_click",
    "b_clicks",
    "b_input",
    "b_inclick",
    "b_outclick",
    "b_stop",
    "b_regs",
    "b_logins",
    "b_active_visitors",
    "b_new_pageviews",
}

var fieldTypes = []FieldType{
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    FLOAT64,
    FLOAT64,
    FLOAT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
    INT64,
}

var dataCountSql = "SELECT count(*) from trend_campaign"
var querySql = "SELECT %s, campaign_id FROM trend_campaign WHERE campaign_id IN (%s) GROUP BY campaign_id LIMIT 20 OFFSET 1000"
var querySql2 = "SELECT %s,campaign_id FROM trend_campaign WHERE campaign_id IN (%s) AND date >= %d AND date <= %d GROUP BY campaign_id LIMIT 20 OFFSET 0"
var querySql3 = "SELECT %s,campaign_id FROM trend_campaign WHERE campaign_id IN (%s) AND hour IN (%s) GROUP BY campaign_id, hour HAVING SUM(cost) > 100  ORDER BY SUM(impressions) desc LIMIT 20 OFFSET 100"

var types = []FieldType{INT, INT, INT, INT, INT, INT}

func (c *MysqlClient) DataCount() int {
    return c.db.QueryScalarInt(dataCountSql)
}

func (c *MysqlClient) Query(cids []int) (rows interface{}, err error) {
    selectStr := ""
    for _, f := range selectFields {
        selectStr += "SUM(`" + f + "`),"
    }
    selectStr = strings.TrimSuffix(selectStr, ",")

    idsStr := ""
    for _, id := range cids {
        idsStr += strconv.Itoa(id) + ","
    }
    idsStr = strings.TrimSuffix(idsStr, ",")

    sql := fmt.Sprintf(querySql, selectStr, idsStr)
    if Debug {
        fmt.Println(sql)
    }
    // fmt.Println("sql ", sql)
    res := c.db.Query(sql)

    // fmt.Println(len(res.Rows))
    return res.Rows, res.Error
}

func (c *MysqlClient) Query2(cids []int, date0, date1 int) (rows interface{}, err error) {
    selectStr := ""
    for _, f := range selectFields {
        selectStr += "SUM(`" + f + "`),"
    }
    selectStr = strings.TrimSuffix(selectStr, ",")

    idsStr := ""
    for _, id := range cids {
        idsStr += strconv.Itoa(id) + ","
    }
    idsStr = strings.TrimSuffix(idsStr, ",")

    sql := fmt.Sprintf(querySql2, selectStr, idsStr, date0, date1)
    if Debug {
        fmt.Println(sql)
    }
    // fmt.Println("sql ", sql)
    res := c.db.Query(sql)
    // fmt.Println(len(res.Rows))
    return res.Rows, res.Error
}

func (c *MysqlClient) Query3(cids []int, hours []int) (rows interface{}, err error) {
    selectStr := ""
    for _, f := range selectFields {
        selectStr += "SUM(`" + f + "`),"
    }
    selectStr = strings.TrimSuffix(selectStr, ",")

    idsStr := ""
    for _, id := range cids {
        idsStr += strconv.Itoa(id) + ","
    }
    idsStr = strings.TrimSuffix(idsStr, ",")

    hourStr := ""
    for _, h := range hours {
        hourStr += strconv.Itoa(h) + ","
    }
    hourStr = strings.TrimSuffix(hourStr, ",")

    sql := fmt.Sprintf(querySql3, selectStr, idsStr, hourStr)
    if Debug {
        fmt.Println(sql)
    }
    // fmt.Println("sql ", sql)
    res := c.db.Query(sql)
    // fmt.Println(len(res.Rows))
    return res.Rows, res.Error
}
