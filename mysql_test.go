package trysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Album struct {
	ID         int64  `colname:"id"`
	Title      string `colname:"title"`
	Artist     string `colname:"artist"`
	Price      float64
	Quantity   int64
	HelloWorld string
	yourName   string
	CreateTime time.Time
}

func Test_MYSQL_AsList(t *testing.T) {
	db, err := initDB()
	if err != nil {
		t.Fatal(err)
	}

	var album = make([]Album, 0)
	newSession := NewTxSession(db, true)
	err = NewMySqlSession(newSession).Select("r.id, title,artist,create_time").
		From("album r").
		Where("artist like #{go}", "你%").
		AsList(&album)
	if err != nil {
		t.Fatal(err)
	}
	for i := range album {
		fmt.Println(album[i])
	}
}

func Test_MYSQL_AsSingle(t *testing.T) {
	db, _ := initDB()

	var album Album

	sqlSession := NewTxSession(db, false)
	single := NewMySqlSession(sqlSession).Select("r.id, title,artist").
		From("album r").
		Where("title <> #{test}", "Test").
		AsSingle(&album)
	fmt.Println(single, album)

}

func Test_MYSQL_AsPrimitiveList(t *testing.T) {
	db, _ := initDB()
	var album []int64
	sqlSession := NewTxSession(db, false)
	err := NewMySqlSession(sqlSession).Select("carrier_id").
		From("acc_tracking_result r").Limit(2).
		AsPrimitiveList(&album)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(album)
}

func Test_MYSQL_InsertSelective(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	err := NewMySqlSession(sqlSession).InsertInto("album").Values("title", "你好的").
		Values("artist", "").
		Values("price", 0).Done()
	if err != nil {
		t.Fatal(err)
	}

}
func Test_MYSQL_InsertOne(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	err := NewMySqlSession(sqlSession).InsertInto("album").IntoColumns("title,artist,price").
		IntoValues("1", "2", "3").Done()
	if err != nil {
		t.Fatal(err)
	}

}

func Test_MYSQL_InsertMany(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	err := NewMySqlSession(sqlSession).InsertInto("album").IntoColumns("title,artist,price").
		IntoMultiValues([][]any{{"1", "展示", 9}, {"11", "2展示", 19}}).Done()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_MYSQL_InsertWithId(t *testing.T) {
	db, _ := initDB()
	ssf := NewSqlSessionFactory(Mysql, db, 300*time.Second, true)
	sqlSession := ssf.NewSqlSession()
	id, err := NewMySqlSession(sqlSession).InsertInto("album").IntoColumns("title,artist,price,create_time").
		IntoValues("1", "2", "3", time.Now()).DoneInsertId("id")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(id)
}

func Test_MYSQL_Update(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	id, err := NewMySqlSession(sqlSession).Update("album").SetSelective("title", "中文测试标题").
		In("id", []any{1, 2, 3}).Where("artist <> #{name}", "TEST").DoneRowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(id)
}

func Test_MYSQL_Delete(t *testing.T) {
	db, _ := initDB()
	sqlSession := NewTxSession(db, false)
	id, err := NewMySqlSession(sqlSession).DeleteFrom("album").In("id", []any{156, 157, 158}).DoneRowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(id)
}

func Test_MYSQL_AsPrimitive(t *testing.T) {
	db, _ := initDB()
	txSession := NewTxSession(db, false)
	var count int64
	sqlSession := NewMySqlSession(txSession)
	err := sqlSession.Select("count(*)").From("acc_user c").Where("id > 300").
		AppendRaw("AND exists(select id from acc_tenant where id = c.main_id)").
		AsPrimitive(&count)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(count)
}

func initDB() (*sql.DB, error) {
	type DbConfig struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Url      string `json:"url"`
		Database string `json:"database"`
	}
	database := make(map[string]DbConfig)
	file, err := ioutil.ReadFile("db_config.json")
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
