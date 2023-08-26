package clickhouse_test

import (
	"context"
	"fmt"
	"github.com/aluka-7/clickhouse"
	"github.com/aluka-7/configuration"
	"github.com/aluka-7/configuration/backends"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var conf configuration.Configuration

func initConfig(t *testing.T) {
	conf = configuration.MockEngine(t, backends.StoreConfig{Exp: map[string]string{
		"/system/base/clickhouse/privileges": "{\"1000\":\"[\"1000\",\"3000\"]\"}",
		"/system/base/clickhouse/common":     "{\"debug\":true,\"dialTimeout\":10,\"maxOpenConns\":10,\"maxIdleConns\":2,\"blockBufferSize\":2,\"maxCompressionBuffer\":10240,\"execTimeout\":5}",
		"/system/base/clickhouse/1000":       "{\"addr\":[\"39.108.108.45:9900\"],\"database\":\"default\",\"username\":\"root\",\"password\":\"8YyZZNWj\"}",
	}})
}

func TestClickHouse(t *testing.T) {
	initConfig(t)
	Convey("test ClickHouse", t, func() {
		eng := clickhouse.Engine(conf, "1000")
		conn := eng.Conn("")
		ctx := context.Background()
		Convey("Test Ping", func() {
			err := conn.Ping(ctx)
			So(err, ShouldBeNil)
		})
		Convey("Test Exec", func() {
			err := conn.Exec(ctx, "DROP TABLE IF EXISTS example")
			So(err, ShouldBeNil)

			err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS example (
				Col1 UInt8, 
				Col2 String, 
				Col3 FixedString(3), 
			    Col4 UUID, 
			    Col5 Map(String, UInt8),
				Col6 Array(String),
				Col7 Tuple(String, UInt8, Array(Map(String, String))),
				Col8 DateTime
			) Engine = Memory`)
			So(err, ShouldBeNil)

			batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
			So(err, ShouldBeNil)
			for i := 0; i < 100; i++ {
				err = batch.Append(
					uint8(42),
					"ClickHouse",
					"Inc",
					uuid.New(),
					map[string]uint8{"key": 1},             // Map(String, UInt8)
					[]string{"Q", "W", "E", "R", "T", "Y"}, // Array(String)
					[]interface{}{ // Tuple(String, UInt8, Array(Map(String, String)))
						"String Value", uint8(5), []map[string]string{
							{"key": "value"},
							{"key": "value"},
							{"key": "value"},
						},
					},
					time.Now(),
				)
				So(err, ShouldBeNil)
			}
			So(batch.Send(), ShouldBeNil)

			rows, err := conn.Query(ctx, "SELECT Col1, Col2, Col3 FROM example WHERE Col1 = 2")
			So(err, ShouldBeNil)
			for rows.Next() {
				var (
					col1 uint8
					col2 string
					col3 string
				)
				err = rows.Scan(&col1, &col2, &col3)
				So(err, ShouldBeNil)

				fmt.Printf("row: col1=%d, col2=%s, col3=%s\n", col1, col2, col3)
			}
		})
	})
}
