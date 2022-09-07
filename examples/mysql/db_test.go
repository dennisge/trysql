package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/dennisge/trysql"
	"github.com/go-sql-driver/mysql"
	"os"
	"testing"
)

func TestNewSqlSession(t *testing.T) {
	InitTidbTestDB("../../db_config.json")

	type TrackingResult struct {
		Id int
	}
	var result TrackingResult

	sqlSession := ssf.NewSqlSession()

	single := sqlSession.Select("id").From("acc_tracking_result").Where("id <> -1").
		Limit(2).AsSingle(&result)
	t.Log(single)

	t.Log(result)
	var id int64

	err := sqlSession.Select("id").From("acc_tracking_result").NotIn("id", []any{1, 2, 3}).
		Limit(1).AsPrimitive(&id)
	if err != nil {
		t.Log(err == sql.ErrNoRows)
		t.Log(err)
	}
	t.Log(id)

}

func TestDoInTx(t *testing.T) {

	InitTidbTestDB("../../db_config.json")
	type TrackingResult struct {
		Id int
	}
	var result TrackingResult

	DoInTx(func(ctx context.Context, session trysql.SqlSession) error {
		single := session.Select("id").From("acc_tracking_result").Where("id <> -1").
			Limit(2).AsSingleContext(ctx, &result)
		t.Log(single)
		t.Log(result)
		var id int64

		err := session.Select("id").From("acc_tracking_no_pool").NotIn("id", []any{1, 2, 3}).
			Limit(1).AsPrimitiveContext(ctx, &id)
		t.Log(id)
		return err
	})

}

// / 测试
func InitTidbTestDB(filename string) {
	type DbConfig struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Url      string `json:"url"`
		Database string `json:"database"`
	}
	database := make(map[string]DbConfig)
	file, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	_ = json.Unmarshal(file, &database)
	config := database["mysql"]
	cfg := mysql.Config{
		User:                 config.User,
		Passwd:               config.Password,
		Net:                  "tcp",
		Addr:                 config.Url,
		DBName:               config.Database,
		Collation:            "utf8mb4_bin",
		AllowNativePasswords: true,
		ParseTime:            true,
		//Loc:                  time.UTC,
	}
	err = InitDB(cfg.FormatDSN(), 10, 10, 10, true)
	if err != nil {
		panic(err)
	}
}
