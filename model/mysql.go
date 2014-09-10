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
    "cost",
    "cost_over",
    "cost_over2",
    "ips",
    "impressions",
    "new_impressions",
    "visitors",
    "new_visitors",
    "reviews",
    "insights",
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

var dataCountSql = "SELECT count(*) from trend_campaign"

// 全指标-IN条件-Limit
var querySql = "SELECT %s, campaign_id FROM trend_campaign WHERE campaign_id IN (%s) GROUP BY campaign_id LIMIT 20 OFFSET 0"

// 全指标-IN+Range条件-Limit
var querySql2 = "SELECT %s,campaign_id FROM trend_campaign WHERE campaign_id IN (%s) AND date >= %d AND date <= %d GROUP BY campaign_id LIMIT 20 OFFSET 0"

// 全指标-IN+IN条件-汇总结果Range条件-Order-Limit+Offset
var querySql3 = "SELECT %s,campaign_id FROM trend_campaign WHERE campaign_id IN (%s) AND hour IN (%s) GROUP BY campaign_id, hour HAVING SUM(cost) > 100  ORDER BY SUM(impressions) desc LIMIT 20 OFFSET 100"

// 全指标-多维度-3个IN条件-GroupBy四个维度-Limit+Offset
var querySql4 = "SELECT %s, campaign_id, product_id, spot_id, whisky_id FROM trend_campaign WHERE campaign_id IN (%s) AND product_id IN(%s) AND spot_id IN (%s) GROUP BY campaign_id, product_id, spot_id, whisky_id LIMIT 50 OFFSET 400"

// 三个指标-Group by三个维度
var querySql5 = "SELECT campaign_id, date, hour, ips, impressions, new_impressions FROM trend_campaign WHERE campaign_id IN(%s) GROUP BY campaign_id, date, hour LIMIT 50 OFFSET 300"

// 汇总数据
var querySql6 = "SELECT %s FROM trend_campaign WHERE campaign_id IN (%s) AND product_id IN(%s) AND spot_id IN (%s) GROUP BY campaign_id, product_id, spot_id"

// 数量
var querySql7 = "SELECT count(*) FROM trend_campaign WHERE campaign_id IN (%s) AND date >= %d AND date <= %d"

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
    return c.doQuery(sql)
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
    return c.doQuery(sql)
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
    return c.doQuery(sql)
}

func (c *MysqlClient) Query4(cids1 []int, cids2 []int, cids3 []int) (rows interface{}, err error) {
    selectStr := ""
    for _, f := range selectFields {
        selectStr += "SUM(`" + f + "`),"
    }
    selectStr = strings.TrimSuffix(selectStr, ",")

    idsStr1 := ""
    for _, id := range cids1 {
        idsStr1 += strconv.Itoa(id) + ","
    }
    idsStr1 = strings.TrimSuffix(idsStr1, ",")

    idsStr2 := ""
    for _, id := range cids2 {
        idsStr2 += strconv.Itoa(id) + ","
    }
    idsStr2 = strings.TrimSuffix(idsStr2, ",")

    idsStr3 := ""
    for _, id := range cids3 {
        idsStr3 += strconv.Itoa(id) + ","
    }
    idsStr3 = strings.TrimSuffix(idsStr3, ",")

    sql := fmt.Sprintf(querySql4, selectStr, idsStr1, idsStr2, idsStr3)
    return c.doQuery(sql)
}

func (c *MysqlClient) Query5(cids []int) (rows interface{}, err error) {
    idsStr := ""
    for _, id := range cids {
        idsStr += strconv.Itoa(id) + ","
    }
    idsStr = strings.TrimSuffix(idsStr, ",")

    sql := fmt.Sprintf(querySql5, idsStr)
    return c.doQuery(sql)
}

func (c *MysqlClient) Query6(cids1 []int, cids2 []int, cids3 []int) (rows interface{}, err error) {
    selectStr := ""
    for _, f := range selectFields {
        selectStr += "SUM(`" + f + "`),"
    }
    selectStr = strings.TrimSuffix(selectStr, ",")

    idsStr1 := ""
    for _, id := range cids1 {
        idsStr1 += strconv.Itoa(id) + ","
    }
    idsStr1 = strings.TrimSuffix(idsStr1, ",")

    idsStr2 := ""
    for _, id := range cids2 {
        idsStr2 += strconv.Itoa(id) + ","
    }
    idsStr2 = strings.TrimSuffix(idsStr2, ",")

    idsStr3 := ""
    for _, id := range cids3 {
        idsStr3 += strconv.Itoa(id) + ","
    }
    idsStr3 = strings.TrimSuffix(idsStr3, ",")

    sql := fmt.Sprintf(querySql6, selectStr, idsStr1, idsStr2, idsStr3)
    return c.doQuery(sql)
}

func (c *MysqlClient) Query7(cids []int, date0, date1 int) (rows interface{}, err error) {
    idsStr := ""
    for _, id := range cids {
        idsStr += strconv.Itoa(id) + ","
    }
    idsStr = strings.TrimSuffix(idsStr, ",")

    sql := fmt.Sprintf(querySql7, idsStr, date0, date1)
    return c.doQuery(sql)
}

func (c *MysqlClient) doQuery(sql string) (rows interface{}, err error) {
    if Debug {
        fmt.Println(sql)
    }
    res := c.db.Query(sql)
    return res.Rows, res.Error
}
