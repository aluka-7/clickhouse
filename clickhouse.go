package clickhouse

import (
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aluka-7/configuration"
	"github.com/aluka-7/utils"
	"github.com/rs/zerolog/log"
	"strings"
	"time"
)

type Config struct {
	Addr                 []string `json:"addr"`                 // ["127.0.0.1:9000"]
	Database             string   `json:"database"`             // 数据库
	Username             string   `json:"username"`             // 用户名
	Password             string   `json:"password"`             // 密码
	Debug                bool     `json:"debug"`                // 开启debug
	DialTimeout          int      `json:"dialTimeout"`          // 连接超时时间，秒
	MaxOpenConns         int      `json:"maxOpenConns"`         // 最大开放连接数
	MaxIdleConns         int      `json:"maxIdleConns"`         // 最大空闲连接数
	BlockBufferSize      uint8    `json:"blockBufferSize"`      // 块缓冲区大小
	MaxCompressionBuffer int      `json:"maxCompressionBuffer"` // 最大压缩值
	ExecTimeout          int      `json:"execTimeout"`          // 执行超时时间，秒
	ConnMaxLifetime      int      `json:"connMaxLifetime"`      // 连接最大生存时间，分
}

type clickHouse struct {
	systemId   string
	cfg        configuration.Configuration
	privileges map[string][]string
}

type ClickHouse interface {
	Config(dsID string) *Config
	Conn(dsID string) driver.Conn
}

func Engine(cfg configuration.Configuration, systemId string) ClickHouse {
	fmt.Println("Loading ClickHouse Engine")
	return &clickHouse{cfg: cfg, systemId: systemId, privileges: make(map[string][]string, 0)}
}

func (d *clickHouse) Config(dsID string) *Config {
	ds, dsID, err := d.getConfiguration(dsID, d.systemId)
	if len(ds.Addr) == 0 || err != nil {
		panic(fmt.Sprintf("数据源[%s]配置未指定或者读取时发生错误:%+v", dsID, err))
	}
	return ds
}

func (d *clickHouse) Conn(dsID string) driver.Conn {
	c := d.Config(dsID)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: c.Addr,
		Auth: clickhouse.Auth{
			Database: c.Database,
			Username: c.Username,
			Password: c.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": c.ExecTimeout,
		},
		DialTimeout: time.Duration(c.DialTimeout) * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionZSTD,
		},
		Debug: c.Debug,
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		BlockBufferSize:      c.BlockBufferSize,
		MaxCompressionBuffer: c.MaxCompressionBuffer,

		MaxOpenConns:     c.MaxOpenConns,
		MaxIdleConns:     c.MaxIdleConns,
		ConnMaxLifetime:  time.Duration(c.ConnMaxLifetime) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		ClientInfo: clickhouse.ClientInfo{ // optional, please see Client info section in the README.md
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "app", Version: dsID},
			},
		},
	})
	if err != nil {
		panic(fmt.Sprintf("初始化clickhouse引擎出错%+v", err))
	}
	return conn
}

func (d *clickHouse) getConfiguration(dsID, csID string) (*Config, string, error) {
	config := &Config{}
	// 如果是获取默认的数据源，则使用当前系统的标示，否则鉴权
	if len(dsID) == 0 || dsID == csID {
		dsID = csID
	} else {
		plist := d.systemPrivileges(csID) // 数据库的访问权限鉴权
		if len(plist) == 0 || utils.ContainsString(plist, dsID) == -1 {
			return config, "", fmt.Errorf("系统[%s]无数据源[%s]的访问权限", csID, dsID)
		}
	}
	err := d.readFromConfiguration(dsID, config)
	return config, dsID, err
}

func (d *clickHouse) systemPrivileges(csID string) []string {
	d.cfg.Get("base", "clickhouse", "", []string{"privileges"}, d)
	plist := d.privileges[csID]
	fmt.Printf("系统[%s]的数据源权限:%s", csID, strings.Join(plist, ","))
	return plist
}

func (d *clickHouse) Changed(data map[string]string) {
	for _, v := range data {
		var vl map[string][]string
		if err := json.Unmarshal([]byte(v), &vl); err == nil {
			for k, _v := range vl {
				d.privileges[k] = _v
			}
		}
	}
}

func (d *clickHouse) readFromConfiguration(dsID string, config *Config) error {
	ex := d.readCommonProperties(config)
	if ex != nil {
		return ex
	}
	fmt.Printf("从配置中心读取数据源配置:/base/clickhouse/%s\n", dsID)
	ex = d.cfg.Clazz("base", "clickhouse", "", dsID, config)
	if ex != nil {
		log.Error().Err(ex).Msgf("数据源[%s]的配置获取失败", dsID)
	}
	return ex
}

func (d *clickHouse) readCommonProperties(config *Config) error {
	fmt.Println("从配置中心的读取通用数据源配置:/base/clickhouse/common")
	vl, err := d.cfg.String("base", "clickhouse", "", "common")
	if err != nil {
		log.Error().Err(err).Msg("配置中心的通用数据源配置获取失败:%v")
	} else {
		if err = json.Unmarshal([]byte(vl), config); err != nil {
			log.Error().Err(err).Msg("解析数据源的通用配置失败")
		}
	}
	return err
}
