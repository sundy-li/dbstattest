package model

import (
    "fmt"
    "math/rand"
    "strings"
    "time"
)

var Fields = []string{
    "campaign_id",
    "product_id",
    "policy_id",
    "package_id",
    "creative_id",
    "spot_id",
    "whisky_id",
    "spot_channel_id",
    "medium_id",
    "channel_type_id",
    "channel_id",
    "platform_id",
    "date",
    "hour",
    "user_id",

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
    "reserve0",
    "reserve1",
    "reserve2",
    "reserve3",
    "reserve4",
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
    "b_reserve0",
    "b_reserve1",
    "b_reserve2",
    "b_reserve3",
    "b_reserve4",
    "b_active_visitors",
    "b_new_pageviews",
}

var (
    table              = "trend_campaign"
    int64FieldIndexs   = []int{5}
    float64FieldIndexs = []int{22, 23, 24}
    tinyIntFieldIndexs = []int{13}
    metricFieldStart   = 15
    batchCount         = 100
)

var Debug = false
var insertSql = "insert into trend_campaign (#FIELDS#) values #VALUES# on duplicate key update #UPDATES#"

func init() {
    rand.Seed(time.Now().Unix())

    insertSql = strings.Replace(insertSql, "#FIELDS#", strings.Join(Fields, ","), 1)
    updates := ""
    for i := metricFieldStart; i < len(Fields); i++ {
        f := Fields[i]
        updates += fmt.Sprintf("%s=%s+values(%s),", f, f, f)
    }
    updates = strings.TrimSuffix(updates, ",")
    insertSql = strings.Replace(insertSql, "#UPDATES#", updates, 1)

    // fmt.Printf("insertSql: %s\n", insertSql)
}

type Campaign map[string]interface{}

func GenCamps() []Campaign {
    camps := make([]Campaign, batchCount)
    for campIdx, _ := range camps {
        camp := make(map[string]interface{}, len(Fields))
        for i, f := range Fields {
            camp[f] = randomValue(i)
        }
        camps[campIdx] = camp
    }
    return camps
}

func GenCampIds(count int) []int {
    ids := make([]int, count)
    for i, _ := range ids {
        ids[i] = rand.Intn(10000)
    }
    return ids
}

func GenDates(count int) []int {
    ids := make([]int, count)
    for i, _ := range ids {
        ids[i] = rand.Intn(10000)
    }
    return ids
}

func GenHours(count int) []int {
    ids := make([]int, count)
    for i, _ := range ids {
        ids[i] = rand.Intn(25)
    }
    return ids
}

func GenDateRange() (date0, date1 int) {
    date0 = rand.Intn(10000)
    date1 = rand.Intn(10000)
    if date0 > date1 {
        tmp := date0
        date0 = date1
        date1 = tmp
    }
    return
}

func randomValue(index int) interface{} {
    if inIntSlice(int64FieldIndexs, index) {
        return rand.Int63n(10000000)
    } else if inIntSlice(float64FieldIndexs, index) {
        return float32(0.5)
    } else if inIntSlice(tinyIntFieldIndexs, index) {
        return rand.Intn(25)
    } else {
        return rand.Intn(10000)
    }
}

func inIntSlice(s []int, v int) bool {
    for _, a := range s {
        if a == v {
            return true
        }
    }
    return false
}

func genInsertSql(camps []Campaign) string {
    valStr := ""
    for _, row := range camps {
        valStr += "("
        for _, f := range Fields {
            v := row[f]
            valStr += fmt.Sprintf("%v", v) + ","
        }
        valStr = strings.TrimSuffix(valStr, ",")
        valStr += "),"
    }
    valStr = strings.TrimSuffix(valStr, ",")
    return strings.Replace(insertSql, "#VALUES#", valStr, 1)
}

type DbClient interface {
    Insert(camp Campaign) error
    InsertBatch(camps []Campaign) (count int64, err error)

    LoadData(fileName string) error

    DataCount() int

    Query(cids []int) (rows interface{}, err error)
    Query2(cids []int, date0, date1 int) (rows interface{}, err error)
    Query3(cids []int, hours []int) (rows interface{}, err error)

    Init()
    Destroy()
    StartDB() error
    StopDB() error
}
