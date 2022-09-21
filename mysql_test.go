package trysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type TrackingResultPo struct {
	ID         int64 `colname:"id"`
	TrackingNo string
	Quantity   int64
	HelloWorld string
	yourName   string
	CreateTime time.Time
}

func Test_MYSQL_AsList(t *testing.T) {
	db, err := initDB()
	if err != nil {
		t.Log(err)
	}

	var po = make([]TrackingResultPo, 0)
	newSession := NewTxSession(db, true)
	err = NewMySqlSession(newSession).Select("r.id,create_time").
		From("acc_tracking_result r").
		Where("tracking_no like #{go}", "你%").
		AsList(&po)
	if err != nil {
		t.Log(err)
	}
	for i := range po {
		fmt.Println(po[i])
	}
}

func Test_MYSQL_AsSingle(t *testing.T) {
	db, _ := initDB()

	var po TrackingResultPo

	sqlSession := NewTxSession(db, false)
	single := NewMySqlSession(sqlSession).Select("r.id, tracking_no, create_time").
		From("acc_tracking_result r").
		Where("title <> #{test}", "Test").
		AsSingle(&po)
	fmt.Println(single, po)

}

func Test_MYSQL_AsList2(t *testing.T) {
	db, _ := initDB()

	type Po struct {
		Id        int64
		CarrierId int64
	}
	var po []Po
	sqlSession := NewTxSession(db, false)
	err := NewMySqlSession(sqlSession).Select("${id}", "carrier_id").
		From("acc_tracking_result r").Where("id > #{id}", 1).
		Where("id < ${iId} and id > #{xId}", 100000, 2).
		AddParam("${id}", "id").
		Limit(2).
		AsList(&po)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(po)
}

func Test_MYSQL_InsertSelective(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	err := NewMySqlSession(sqlSession).InsertInto("acc_tracking_result").Values("title", "你好的").
		Values("artist", "").
		Values("price", 0).Done()
	if err != nil {
		t.Log(err)
	}

}
func Test_MYSQL_InsertOne(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	err := NewMySqlSession(sqlSession).InsertInto("acc_tracking_result").IntoColumns("title,artist,price").
		IntoValues("1", "2", "3").Done()
	if err != nil {
		t.Log(err)
	}
}

func Test_MYSQL_InsertMany(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	err := NewMySqlSession(sqlSession).InsertInto("acc_tracking_result").IntoColumns("title,artist,price").
		IntoMultiValues([][]any{{"1", "展示", 9}, {"11", "2展示", 19}}).Done()
	if err != nil {
		t.Log(err)
	}
}

func Test_MYSQL_InsertWithId(t *testing.T) {
	db, _ := initDB()
	ssf := NewSqlSessionFactory(Mysql, db, 300*time.Second, true)
	sqlSession := ssf.NewSqlSession()
	id, err := NewMySqlSession(sqlSession).InsertInto("acc_tracking_result").IntoColumns("title,artist,price,create_time").
		IntoValues("1", "2", "3", time.Now()).DoneInsertId("id")
	if err != nil {
		t.Log(err)
	}
	fmt.Println(id)
}

func Test_MYSQL_Update(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	id, err := NewMySqlSession(sqlSession).Update("acc_tracking_result").SetSelective("title", "中文测试标题").
		WhereIn("id", []any{-1, -2, -3}).Where("artist <> #{name}", "TEST").DoneRowsAffected()
	if err != nil {
		t.Log(err)
	}
	fmt.Println(id)
}

func Test_MYSQL_Delete(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	id, err := NewMySqlSession(sqlSession).DeleteFrom("acc_tracking_result").WhereIn("id", []any{-156, -157, -158}).DoneRowsAffected()
	if err != nil {
		t.Log(err)
	}
	fmt.Println(id)
}

func Test_MYSQL_AsPrimitive(t *testing.T) {
	db, _ := initDB()
	txSession := NewTxSession(db, false)
	var count int64
	sqlSession := NewMySqlSession(txSession)
	err := sqlSession.Select("count(*)").From("acc_tracking_result c").Where("id > 300").
		AppendRaw("AND exists(select id from acc_tracking_result where id > 100)").
		AsPrimitive(&count)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(count)
}

func Test_MYSQL_AsMap(t *testing.T) {
	db, _ := initDB()

	sqlSession := NewTxSession(db, false)
	m, err := NewMySqlSession(sqlSession).Select("*").
		From("acc_user r").Limit(1).
		AsMap()
	if err != nil {
		t.Error(err)
	}

	t.Logf("%v", m)
}

func Test_MYSQL_AsMapList(t *testing.T) {
	db, _ := initDB()

	sqlSession := NewTxSession(db, false)
	m, err := NewMySqlSession(sqlSession).Select("*").
		From("acc_user r").Limit(2).
		AsMapList()
	if err != nil {
		t.Error(err)
	}
	for _, v := range m {
		t.Logf("%v", v)
	}
}

func initDB() (*sql.DB, error) {
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
		ParseTime:            true,
	}
	enabledLogSql(true)
	open, err := sql.Open("mysql", cfg.FormatDSN())

	return open, err
}
