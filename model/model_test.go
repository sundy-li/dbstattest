package model

import (
    "fmt"
    // "fmt"
    elastigo "github.com/mattbaird/elastigo/lib"
    . "github.com/smartystreets/goconvey/convey"
    "testing"
)

func TestMysql(t *testing.T) {
    Convey("TestMysql", t, func() {
        return
        mysqlc := NewInfoBrightClient()
        mysqlc.Init()
        count, err := mysqlc.InsertBatch(GenCamps())
        So(err, ShouldEqual, nil)
        So(count, ShouldEqual, batchCount)

        cids := GenCampIds(10)
        _, err = mysqlc.Query(cids)
        So(err, ShouldEqual, nil)

        date0, date1 := GenDateRange()
        _, err = mysqlc.Query2(cids, date0, date1)
        So(err, ShouldEqual, nil)

        cids = GenCampIds(10)
        _, err = mysqlc.Query3(cids, cids)
        So(err, ShouldEqual, nil)

    })
}

func TestES(t *testing.T) {
    Convey("TestES", t, func() {
        // return
        elastigo.ESDebug = true
        esc := NewESClient()
        esc.Init()

        fmt.Printf("count: %d", esc.DataCount())
        // _, err := esc.InsertBatch(GenCamps())
        // So(err, ShouldEqual, nil)
        // cResp, err := esc.conn.Count(esindex, estype, nil)
        // So(err, ShouldEqual, nil)
        // fmt.Printf("count: %d\n", cResp.Count)
        cids := GenCampIds(20)
        _, err := esc.Query(cids)
        So(err, ShouldEqual, nil)

        fmt.Printf("size: %v, count %v\n", RsbyteCount, RsAggCount)

        date0, date1 := GenDateRange()
        _, err = esc.Query2(cids, date0, date1)
        So(err, ShouldEqual, nil)

        cids = GenCampIds(10)
        _, err = esc.Query3(cids, cids)
        So(err, ShouldEqual, nil)
    })
}
