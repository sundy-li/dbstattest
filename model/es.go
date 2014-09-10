package model

import (
    "encoding/json"
    "errors"
    "fmt"

    // "fmt"
    "github.com/belogik/goes"
    elastigo "github.com/mattbaird/elastigo/lib"
)

type M map[string]interface{}

var (
    eshost1 = "192.168.10.60"
    // eshost2 = "192.168.10.61"
    esindex = "estest"
    estype  = "trend_campaign"
)

var RsbyteCount = 0
var RsAggCount = 0

type ESClient struct {
    conn      *elastigo.Conn
    batchconn *goes.Connection
}

func NewESClient() DbClient {
    conn := elastigo.NewConn()
    host := eshost1
    conn.Domain = host

    batchconn := goes.NewConnection(host, "9200")
    return &ESClient{
        conn:      conn,
        batchconn: batchconn,
    }
}

func (c *ESClient) Init() {

}

func (c *ESClient) Destroy() {

}

func (c *ESClient) StartDB() error {
    return errors.New("Unsupported")
}

func (c *ESClient) StopDB() error {
    return errors.New("Unsupported")
}

func (c *ESClient) LoadData(fileName string) error {
    return errors.New("Unsupported")
}

func (c *ESClient) InsertBatch(camps []Campaign) (count int64, err error) {
    docs := make([]goes.Document, len(camps))
    for i, c := range camps {
        docs[i] = goes.Document{
            Index:       esindex,
            Type:        estype,
            BulkCommand: goes.BULK_COMMAND_INDEX,
            Fields:      c,
        }
    }
    _, err = c.batchconn.BulkSend(docs)
    return
}

func (c *ESClient) Insert(camp Campaign) error {
    _, err := c.conn.Index(esindex, estype, "", nil, camp)
    return err
}

func (c *ESClient) DataCount() int {
    cr, _ := c.conn.Count(esindex, estype, nil)
    return cr.Count
}

func (c *ESClient) Query(cids []int) (rows interface{}, err error) {
    query := M{
        "size": 0,
        "query": M{
            "filtered": M{
                "filter": M{
                    "and": []interface{}{
                        M{"terms": M{"campaign_id": cids}},
                    },
                },
            },
        },
        "aggs": M{
            "group_by_campaign_id": M{
                "terms": M{
                    "field": "campaign_id",
                    "order": M{"impressions": "desc"},
                    "size":  20,
                },
                "aggs": M{
                    // "ips":         M{"max": M{"field": "ips"}},
                    "impressions": M{"max": M{"field": "impressions"}},
                    // "visitors":    M{"max": M{"field": "visitors"}},

                    // "clicks":      M{"max": M{"field": "clicks"}},
                    // "cost":        M{"max": M{"field": "cost"}},
                    // "b_regs":      M{"max": M{"field": "b_regs"}},
                    // "b_pageviews": M{"max": M{"field": "b_pageviews"}},
                    // "date":        M{"avg": M{"field": "date"}},

                    "reviews":        M{"max": M{"field": "reviews"}},
                    "insights":       M{"max": M{"field": "insights"}},
                    "cost":           M{"max": M{"field": "cost"}},
                    "cost_over":      M{"max": M{"field": "cost_over"}},
                    "cost_over2":     M{"max": M{"field": "cost_over2"}},
                    "pagepixels":     M{"max": M{"field": "pagepixels"}},
                    "clicks":         M{"max": M{"field": "clicks"}},
                    "spot_screen_id": M{"max": M{"field": "spot_screen_id"}},
                    // "b_ips":           M{"max": M{"field": "b_ips"}},
                    "b_pageviews":    M{"max": M{"field": "b_pageviews"}},
                    "b_sessions":     M{"max": M{"field": "b_sessions"}},
                    "b_visitors":     M{"max": M{"field": "b_visitors"}},
                    "b_new_visitors": M{"max": M{"field": "b_new_visitors"}},
                    "b_bounces":      M{"max": M{"field": "b_bounces"}},
                    "b_pagepixels":   M{"max": M{"field": "b_pagepixels"}},
                    "b_staytime":     M{"max": M{"field": "b_staytime"}},
                    "b_loadtime":     M{"max": M{"field": "b_loadtime"}},
                    "b_click":        M{"max": M{"field": "b_click"}},
                    "b_clicks":       M{"max": M{"field": "b_clicks"}},
                    "b_input":        M{"max": M{"field": "b_input"}},
                    "b_inclick":      M{"max": M{"field": "b_inclick"}},
                    "b_outclick":     M{"max": M{"field": "b_outclick"}},
                    "b_stop":         M{"max": M{"field": "b_stop"}},
                    "b_regs":         M{"max": M{"field": "b_regs"}},
                    "b_logins":       M{"max": M{"field": "b_logins"}},
                },
            },
        },
    }
    sr, err := c.conn.Search(esindex, estype, nil, query)

    RsbyteCount += len(sr.RawJSON)
    v := map[string]interface{}{}
    json.Unmarshal([]byte(sr.Aggregations), &v)
    // sr.Aggregations.UnmarshalJSON(&v)
    mm := v["group_by_campaign_id"]
    if mm == nil {
        return nil, nil
    }
    m := mm.(map[string]interface{})
    bb := m["buckets"]
    if bb == nil {
        return nil, nil
    }
    b := bb.([]interface{})
    RsAggCount += len(b)
    // fmt.Println("RsAggCount", RsAggCount)

    if sr.ShardStatus.Failed > 0 {
        return nil, errors.New(fmt.Sprintf("%v", sr.String()))
    }
    return nil, err
}

func (c *ESClient) Query2(cids []int, date0, date1 int) (rows interface{}, err error) {
    query := M{
        "size": 0,
        "query": M{
            "filtered": M{
                "filter": M{
                    "and": []interface{}{
                        M{"terms": M{"campaign_id": cids}},
                        M{"range": M{"date": M{"gte": date0, "lte": date1}}},
                    },
                },
            },
        },
        "aggs": M{
            "group_by_campaign_id": M{
                "terms": M{
                    "field": "campaign_id",
                    "order": M{"impressions": "desc"},
                    "size":  20,
                },
                "aggs": M{
                    "impressions": M{"max": M{"field": "impressions"}},
                    "clicks":      M{"max": M{"field": "clicks"}},
                    "cost":        M{"max": M{"field": "cost"}},
                    "b_regs":      M{"max": M{"field": "b_regs"}},
                    "b_pageviews": M{"max": M{"field": "b_pageviews"}},
                    "date":        M{"avg": M{"field": "date"}},

                    // "reviews":        M{"max": M{"field": "reviews"}},
                    // "insights":       M{"max": M{"field": "insights"}},
                    // "cost_over":      M{"max": M{"field": "cost_over"}},
                    // "cost_over2":     M{"max": M{"field": "cost_over2"}},
                    // "pagepixels":     M{"max": M{"field": "pagepixels"}},
                    // "spot_screen_id": M{"max": M{"field": "spot_screen_id"}},
                    // // "b_ips":           M{"max": M{"field": "b_ips"}},
                    // "b_sessions":     M{"max": M{"field": "b_sessions"}},
                    // "b_visitors":     M{"max": M{"field": "b_visitors"}},
                    // "b_new_visitors": M{"max": M{"field": "b_new_visitors"}},
                    // "b_bounces":      M{"max": M{"field": "b_bounces"}},
                    // "b_pagepixels":   M{"max": M{"field": "b_pagepixels"}},
                    // "b_staytime":     M{"max": M{"field": "b_staytime"}},
                    // "b_loadtime":     M{"max": M{"field": "b_loadtime"}},
                    // "b_click":        M{"max": M{"field": "b_click"}},
                    // "b_clicks":       M{"max": M{"field": "b_clicks"}},
                    // "b_input":        M{"max": M{"field": "b_input"}},
                    // "b_inclick":      M{"max": M{"field": "b_inclick"}},
                    // "b_outclick":     M{"max": M{"field": "b_outclick"}},
                    // "b_stop":         M{"max": M{"field": "b_stop"}},
                    // "b_logins":       M{"max": M{"field": "b_logins"}},
                },
            },
        },
    }
    sr, err := c.conn.Search(esindex, estype, nil, query)

    RsbyteCount += len(sr.RawJSON)
    v := map[string]interface{}{}
    json.Unmarshal([]byte(sr.Aggregations), &v)
    // sr.Aggregations.UnmarshalJSON(&v)
    mm := v["group_by_campaign_id"]
    if mm == nil {
        return nil, nil
    }
    m := mm.(map[string]interface{})
    bb := m["buckets"]
    if bb == nil {
        return nil, nil
    }
    b := bb.([]interface{})
    RsAggCount += len(b)

    if sr.ShardStatus.Failed > 0 {
        return nil, errors.New(fmt.Sprintf("%v", sr.String()))
    }
    // fmt.Println(res.Hits.Total)
    return nil, err
}

func (c *ESClient) Query3(cids []int, hours []int) (rows interface{}, err error) {
    query := M{
        "size": 0,
        "query": M{
            "filtered": M{
                "filter": M{
                    "and": []interface{}{
                        M{"terms": M{"campaign_id": cids}},
                        M{"terms": M{"hour": hours}},
                    },
                },
            },
        },
        "aggs": M{
            "group_by_campaign_id": M{
                "terms": M{
                    "field": "campaign_id",
                    "size":  20,
                },
                "aggs": M{
                    "group_by_hour": M{
                        "terms": M{
                            "field": "hour",
                            "order": M{"impressions": "desc"},
                            "size":  20,
                        },
                        "aggs": M{
                            "impressions": M{"max": M{"field": "impressions"}},
                            "clicks":      M{"max": M{"field": "clicks"}},
                            "cost":        M{"max": M{"field": "cost"}},
                            "b_regs":      M{"max": M{"field": "b_regs"}},
                            "b_pageviews": M{"max": M{"field": "b_pageviews"}},
                            "hour":        M{"avg": M{"field": "hour"}},
                        },
                    },
                },
            },
        },
    }
    sr, err := c.conn.Search(esindex, estype, nil, query)

    RsbyteCount += len(sr.RawJSON)
    v := map[string]interface{}{}
    json.Unmarshal([]byte(sr.Aggregations), &v)
    // sr.Aggregations.UnmarshalJSON(&v)
    mm := v["group_by_campaign_id"]
    if mm == nil {
        return nil, nil
    }
    m := mm.(map[string]interface{})
    bb := m["buckets"]
    if bb == nil {
        return nil, nil
    }
    b := bb.([]interface{})
    RsAggCount += len(b)

    if sr.ShardStatus.Failed > 0 {
        return nil, errors.New(fmt.Sprintf("%v", sr.String()))
    }

    // fmt.Println(res.Hits.Total)
    return nil, err
}

func (c *ESClient) Query4(cids1 []int, cids2 []int, cids3 []int) (rows interface{}, err error) {
    return nil, errors.New("Unsupported")
}

func (c *ESClient) Query5(cids []int) (rows interface{}, err error) {
    return nil, errors.New("Unsupported")
}

func (c *ESClient) Query6(cids1 []int, cids2 []int, cids3 []int) (rows interface{}, err error) {
    return nil, errors.New("Unsupported")
}

func (c *ESClient) Query7(cids []int, date0, date1 int) (rows interface{}, err error) {
    return nil, errors.New("Unsupported")
}
