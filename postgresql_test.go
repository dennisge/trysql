package trysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

var sDB *sql.DB

func Test_PG_Select(t *testing.T) {
	initPostgresqlDB()
	type BasePo struct {
		ID    int64 `colname:"id"`
		Email string
	}
	type Tenant1Po struct {
		*BasePo    // 指针嵌入
		TenantName string
	}
	var tenant1 Tenant1Po
	db := NewTxSession(sDB, false)
	err := NewPostgreSqlSession(db).Select("tenant_name , email ,id ,default_lan").
		From("acc_tenant r").Limit(1).Offset(2).
		AsSingle(&tenant1)
	if err != nil {
		t.Log(err)
	}
	t.Logf("结果：%v,Base=%v", tenant1, tenant1.BasePo)

	type Tenant2Po struct {
		BasePo     // 常规嵌入
		TenantName string
	}

	var tenant2 Tenant2Po
	NewPostgreSqlSession(db).Select("tenant_name , email ,id ,default_lan").
		From("acc_tenant r").Limit(1).Offset(2).
		AsSingle(&tenant2)
	t.Logf("结果：%v,Base=%v", tenant2, tenant2.BasePo)

	var tenants = make([]Tenant1Po, 0)
	NewPostgreSqlSession(db).Select("tenant_name , email ,id ,default_lan").
		From("acc_tenant r").Limit(1).Offset(2).
		AsList(&tenants)
	t.Logf("结果：%v", tenants)
	if len(tenants) > 0 {
		t.Logf("结果[0]：%v,Base=%v", tenants[0], tenants[0].BasePo)
	}
}

func Test_PG_SelectPrimitiveList(t *testing.T) {
	initPostgresqlDB()

	var album []int64
	db := NewTxSession(sDB, false)
	err := NewPostgreSqlSession(db).Select("carrier_id").
		From("acc_tracking_result r").Limit(2).Offset(2).
		AsPrimitiveList(&album)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(album[0])
}

func Test_PG_Update(t *testing.T) {
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
	config := database["postgresql"]
	dsn := "postgresql://" + config.User + ":" + config.Password + "@" + config.Url + "/" + config.Database + "?connect_timeout=10&sslmode=disable"

	factory, err := NewSqlSessionFactoryByDSN(Postgresql, dsn, 10, 10, 30*time.Second, 30*time.Second, 30*time.Second, true)
	if err != nil {
		t.Log(err)
	}
	exec, err := factory.NewSqlSession().Update("acc_tracking_result").Where("id = #{id}", -1).
		Set("from_address", "").
		SetSelective("XXX", "").
		SetSelective("update_time", time.Now().UTC()).
		DoneRowsAffected()
	if err != nil {
		t.Log(err)
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
		t.Log(err)
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
		t.Log(err)
	}
	t.Log("记录Id =", id)
	var result int64
	err = session.Reset().Select("id").From("stat_kpi").Limit(1).Offset(2).AsPrimitive(&result)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(result)
}

func TestPostgreSqlSession_Select(t *testing.T) {
	var sqlText = `
 	date_trunc('day',now()) time_2,
	now() at time zone 'US/Samoa' as time_3,
                date_trunc('day',now() at time zone #{tz}) as time_4,
                date_trunc('day',now() at time zone #{tz})::timestamptz at time zone #{tz} as time_5,
								now() at time zone 'US/Samoa' time_1
  `

	initPostgresqlDB()
	db := NewTxSession(sDB, false)
	session := NewPostgreSqlSession(db)

	type T struct {
		Time1 time.Time
		Time2 time.Time
		Time3 time.Time
		Time4 time.Time
		Time5 time.Time
	}

	var x T
	session.Select(sqlText).AddParam("#{tz}", "US/Samoa").AsSingle(&x)
	fmt.Println(x)
	fmt.Println(x.Time4.Local())
	fmt.Println(x.Time4.UTC())
	fmt.Println(x.Time5.Local())
	fmt.Println(x.Time5.UTC())
	fmt.Println(x.Time5)

}

func initPostgresqlDB() {
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
	config := database["postgresql"]
	dsn := "postgresql://" + config.User + ":" + config.Password + "@" + config.Url + "/" + config.Database + "?connect_timeout=10&sslmode=disable"
	sDB, err = sql.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}
	enabledLogSql(true)
}
