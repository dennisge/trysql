package trysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

var sDB *sql.DB

func Test_PG_Select(t *testing.T) {
	initPostgresqlDB()
	type Album struct {
		ID         int64  `colname:"carrier_id"`
		Title      string `colname:"title"`
		Artist     string `colname:"artist"`
		Price      float64
		Quantity   int64
		HelloWorld string
		yourName   string
	}
	var album Album
	db := NewTxSession(sDB, false)
	NewPostgreSqlSession(db).Select("carrier_id", "count(r.id) as price", "#{test} as artist").
		From("acc_tracking_result r").Limit(1).Offset(2).
		Where("r.id > 10").
		InnerJoin("acc_tenant t ON t.id = r.tenant_id").
		Having("count(r.id) > #{count}", 3).
		GroupBy("carrier_id").OrderBy("price desc").AddParam("#{test}", "辉哥").AddParam("#{test}", "打个").
		AsSingle(&album)
	fmt.Println(album)
}

func Test_PG_Update(t *testing.T) {
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
	config := database["postgresql"]
	dsn := "postgresql://" + config.User + ":" + config.Password + "@" + config.Url + "/" + config.Database + "?connect_timeout=10&sslmode=disable"

	factory, err := NewSqlSessionFactoryByDSN(Postgresql, dsn, 10, 10, 30*time.Second, 30*time.Second, 30*time.Second, true)
	if err != nil {
		t.Fatal(err)
	}
	exec, err := factory.NewSqlSession().Update("acc_tracking_result").Where("id = #{id}", -1).
		Set("from_address", "").
		SetSelective("XXX", "").
		SetSelective("update_time", time.Now().UTC()).
		DoneRowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(exec)
}

func Test_Pg_Insert(t *testing.T) {
	initPostgresqlDB()
	db := NewTxSession(sDB, false)
	id, err := NewPostgreSqlSession(db).InsertInto("stat_kpi").
		Values("tenant_id", 125).
		Values("carrier_id", 6).
		Values("country_id", 1).
		Values("rate", 0.3465).
		Values("day", 7).
		Values("creator", "UT").
		AppendRaw("ON conflict(tenant_id,carrier_id,country_id) DO UPDATE SET rate = #{rate} ,day = 3, modifier = 'abcd',status = true", 0.7997).
		DoneInsertId("id")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("记录Id =", id)
}

func Test_Pg_Delete(t *testing.T) {
	initPostgresqlDB()
	db := NewTxSession(sDB, false)
	session := NewPostgreSqlSession(db)
	id, err := session.DeleteFrom("stat_kpi").
		Where("id < #{id}", 3).
		Where("id < 0").
		Where("id < #{hello} and carrier_id < #{yes}", 0, 0).
		WhereSelective("carrier_id <0", 0).
		DoneRowsAffected()

	if err != nil {
		t.Fatal(err)
	}
	t.Log("记录Id =", id)
	var result int64
	err = session.Reset().Select("id").From("stat_kpi").Limit(1).Offset(2).AsPrimitive(&result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(result)
}

func initPostgresqlDB() {
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
	config := database["postgresql"]
	dsn := "postgresql://" + config.User + ":" + config.Password + "@" + config.Url + "/" + config.Database + "?connect_timeout=10&sslmode=disable"
	sDB, err = sql.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}
	enabledLogSql(true)
}
