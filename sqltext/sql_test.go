package sqltext

import (
	"fmt"
	"testing"
)

func TestSelect(t *testing.T) {
	sql := NewSQL()
	sql.Select("user_name")
	sql.Select("login_name", "creator,create_time")
	sql.From("user u")
	sql.InnerJoin("tenant t ON u.main_id = t.id")
	sql.RightOuterJoin("test t on t.id = u.id")
	sql.Where("u.id = #{id}")
	sql.Where("u.update > 3")
	sql.Or()
	sql.Where("u.login_name=#{login_name}")
	sql.Where("u.age < 3")
	sql.And()
	sql.Where("h=3")
	sql.GroupBy("u.id,u.name")
	sql.Having("count(id) > 0")
	sql.Where("t.=3")
	sql.OrderBy("id,name")
	sql.Limit("10")
	sql.Offset("2")
	fmt.Println(sql.String())
}

func TestInsertValue(t *testing.T) {
	sql := NewSQL()
	sql.InsertInto("user")
	sql.Values("ID, FIRST_NAME", "#{id}, #{firstName}")
	sql.Values("LAST_NAME", "#{lastName}")
	sql.IntoColumns("col1")
	sql.IntoValues("val3")
	sql.Where("id=ta")
	fmt.Println(sql.String())
}

func TestInsertMultiRows(t *testing.T) {
	sql := NewSQL()
	sql.InsertInto("user")
	sql.IntoColumns("ID, FIRST_NAME", "LAST_NAME")
	sql.IntoValues("#value#", "33", "44")
	sql.AddRow()
	sql.IntoValues("#value#", "33", "44")
	fmt.Println(sql.String())
}

func TestUpdate(t *testing.T) {
	sql := NewSQL()
	sql.Update("user a")
	sql.Set("a.username =#name")
	sql.Set("a.user=3434")
	sql.Where("a.user-id= 34")
	fmt.Println(sql.String())
}

func TestDelete(t *testing.T) {
	sql := NewSQL()
	sql.DeleteFrom("ab")
	sql.Set("a.user=3434")
	sql.Where("a.user-id= 34")
	sql.GroupBy("t")
	sql.Or()
	sql.Where("ddf=3")
	fmt.Println(sql.String())
}
