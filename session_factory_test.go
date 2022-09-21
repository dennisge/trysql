package trysql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"os"
	"testing"
	"time"
)

func TestNewSqlSessionFactory(t *testing.T) {
	type DbConfig struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Url      string `json:"url"`
		Database string `json:"database"`
	}
	database := make(map[string]DbConfig)
	file, err := os.ReadFile("db_config.json")
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
	}

	ssf, err := NewSqlSessionFactoryByDSN(Mysql, cfg.FormatDSN(), 1, 1, 30*time.Second, 30*time.Second, 30*time.Second, true)
	if err != nil {
		t.Log(err)
	}

	enabledLogSql(true)
	type User struct {
		LoginName string `colname:"login_name"`
	}

	users := make([]User, 0)

	ctx, cancelFunc := ssf.NewTimeoutContext(context.Background())
	defer cancelFunc()
	x := ssf.NewSqlSession().Select("login_name").From("acc_user").Limit(10).Offset(2).AsListContext(ctx, &users)
	if x != nil {
		t.Log(x)
	}
	fmt.Println(users)
}

func Test_MYSQL_DoInTx(t *testing.T) {
	type DbConfig struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Url      string `json:"url"`
		Database string `json:"database"`
	}
	database := make(map[string]DbConfig)
	file, err := os.ReadFile("db_config.json")
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
	}
	factory, err := NewSqlSessionFactoryByDSN(Mysql, cfg.FormatDSN(), 10, 10, 30*time.Second, 3*time.Second, 3*time.Second, true)

	if err != nil {
		t.Log(err)
	}

	err = factory.DoInTxContext(context.TODO(), func(ctx context.Context, sqlSession SqlSession) error {
		updated, err := sqlSession.Update("album").SetSelective("title", "3555").
			WhereIn("id", []any{3}).Where("artist <> #{name}", "TEST").DoneRowsAffectedContext(ctx)
		if err != nil {
			t.Log(err)
		}
		fmt.Println("更新：", updated)

		affected, err := sqlSession.Update("album").SetSelective("title", "222222").
			WhereIn("id", []any{5}).Where("artist <> #{name}", "TEST").DoneRowsAffectedContext(ctx)

		if err != nil {
			return err
		}
		fmt.Println("更新：", affected)
		return nil
	})

	fmt.Println(err)

}

func Test_MYSQL_TX(t *testing.T) {
	type DbConfig struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Url      string `json:"url"`
		Database string `json:"database"`
	}
	database := make(map[string]DbConfig)
	file, err := os.ReadFile("db_config.json")
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
	}
	factory, err := NewSqlSessionFactoryByDSN(Mysql, cfg.FormatDSN(), 10, 10, 30*time.Second, 30*time.Second, 30*time.Second, true)

	if err != nil {
		t.Log(err)
	}

	timeoutContext, cancelFunc := factory.NewTimeoutContext(context.TODO(), 3*time.Second)
	defer cancelFunc()
	dbSession := factory.NewTxDbSessionContext(timeoutContext, nil)
	defer func(dbSession DbSession) {
		_ = dbSession.Rollback()
	}(dbSession)
	sqlSession := factory.NewTxSqlSession(dbSession)

	updated, err := sqlSession.Update("album").SetSelective("title", "111").
		WhereIn("id", []any{3}).Where("artist <> #{name}", "TEST").DoneRowsAffectedContext(timeoutContext)
	if err != nil {
		t.Log(err)
	}
	fmt.Println("更新：", updated)
	affected, err := sqlSession.Update("album").SetSelective("title", "66766").
		WhereIn("id", []any{5}).Where("artist2 <> #{name}", "TEST").DoneRowsAffectedContext(timeoutContext)
	fmt.Println("更新：", affected)
	if err != nil {
		fmt.Println(err)
	} else {
		_ = dbSession.Commit()
	}

}
