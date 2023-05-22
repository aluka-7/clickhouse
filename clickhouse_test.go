package clickhouse_test

import (
	"context"
	"fmt"
	"github.com/aluka-7/clickhouse"
	"github.com/aluka-7/configuration"
	"github.com/aluka-7/configuration/backends"
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
		conn := clickhouse.Engine(conf, "1000").Conn("")
		ctx := context.Background()
		Convey("Test Ping", func() {
			err := conn.Ping(ctx)
			So(err, ShouldBeNil)
		})
		Convey("Test Exec", func() {
			err := conn.Exec(ctx, "DROP TABLE IF EXISTS example")
			So(err, ShouldBeNil)

			err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS hc_user_bind (
				user_id UInt64, 
				parent_uid UInt64, 
				created_time DateTime
			) ENGINE = MergeTree() PARTITION BY parent_uid PRIMARY KEY user_id`)
			So(err, ShouldBeNil)

			batch, err := conn.PrepareBatch(ctx, "INSERT INTO hc_user_bind")
			So(err, ShouldBeNil)
			for i := 0; i < 5; i++ {
				err = batch.Append(
					uint64(i),
					uint64(i+10),
					time.Now(),
				)
				So(err, ShouldBeNil)
			}
			So(batch.Send(), ShouldBeNil)

			rows, err := conn.Query(ctx, "SELECT user_id, parent_uid, created_time FROM hc_user_bind WHERE user_id = 1")
			So(err, ShouldBeNil)
			for rows.Next() {
				var (
					userId      uint64
					parentUid   uint64
					createdTime time.Time
				)
				err = rows.Scan(&userId, &parentUid, &createdTime)
				So(err, ShouldBeNil)

				fmt.Printf("row: user_id=%d, parent_uid=%d, created_time=%s\n", userId, parentUid, createdTime)
			}
		})
	})
}
