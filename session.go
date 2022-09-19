package trysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/dennisge/trysql/sqltext"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/iancoleman/strcase"
)

// SqlSession 用于构建 SQL,非线程安全
type SqlSession interface {

	// Select  构建 Select 查询的列
	Select(columns ...string) SqlSession

	// From  构建 Select 的 From 子句
	From(tables ...string) SqlSession

	// Where  构建 Select, Update, Delete 的 Where 子句
	Where(condition string, args ...any) SqlSession

	// WhereSelective  当 arg 不为 零值时，构建 Select, Update, Delete 的 Where 子句
	WhereSelective(condition string, arg any) SqlSession

	// In  当 args 不空时，构建 Where 子句 IN 表达式
	In(column string, args []any) SqlSession

	// NotIn 当 args 不空时， 构建 Where 子句 NotIn 表达式
	NotIn(column string, args []any) SqlSession

	// GroupBy  构建 GroupBy 子句
	GroupBy(columns ...string) SqlSession

	// Having  构建 Having 子句
	Having(condition string, value any) SqlSession

	// OrderBy  构建 OrderBy 子句
	OrderBy(columns ...string) SqlSession

	// InsertInto  构建 Insert 的 表
	InsertInto(table string) SqlSession

	// Values  构建 Insert 的 Value 子句
	Values(column string, value any) SqlSession

	// ValuesSelective  当 value 不为零值时，构建 Insert 的 Value 子句
	ValuesSelective(column string, value any) SqlSession

	// IntoColumns  构建 Insert 的 INTO 子句
	IntoColumns(columns ...string) SqlSession

	// IntoValues  构建 Insert 的 VALUES 子句
	IntoValues(values ...any) SqlSession

	// IntoMultiValues  批量构建 Insert 的 VALUES 子句，列: INSERT INTO A(ID,NAME) VALUES (1,'1'),(2,'2'),(3,'3')
	IntoMultiValues(values [][]any) SqlSession

	// Update 构建 Update 的表
	Update(table string) SqlSession

	// Set 构建 Update 的 Set 子句
	Set(column string, value any) SqlSession

	// SetSelective 当value 不为零值时， 构建 Update 的 Set 子句
	SetSelective(column string, value any) SqlSession

	// DeleteFrom 构建 Delete 的表
	DeleteFrom(table string) SqlSession

	// InnerJoin 构建 INNER JOIN 的表对象
	InnerJoin(joins ...string) SqlSession

	// InnerJoinSelective 当 condition 不为零值时， 构建 INNER JOIN 的表对象
	InnerJoinSelective(join string, condition any) SqlSession

	// LeftOuterJoin 构建 LEFT OUTER JOIN 的表对象
	LeftOuterJoin(joins ...string) SqlSession

	// RightOuterJoin 构建 LEFT OUTER JOIN 的表对象
	RightOuterJoin(joins ...string) SqlSession

	// OuterJoin 构建 OUTER JOIN 的表对象
	OuterJoin(joins ...string) SqlSession

	// Or 构建 Where 子句的 OR 表达式
	Or() SqlSession

	// And 构建 Where 子句的 AND 表达式
	And() SqlSession

	// Limit 构建 SELECT 的 LIMIT 子句
	Limit(limit int) SqlSession

	// Offset 构建 SELECT 的 Offset 子句
	Offset(offset int) SqlSession

	// AddParam 单独添加 SQL 动态参数值
	AddParam(param string, value any) SqlSession

	// AppendRaw 在 SqlSession 自动构建的 SQL 之后，追加 SQL
	AppendRaw(rawSql string, args ...any) SqlSession

	// DoneContext 执行 SQL
	DoneContext(ctx context.Context) error

	// Done 执行 SQL
	Done() error

	// DoneInsertIdContext 执行 INSERT SQL， 同时返回插入记录的 Id
	DoneInsertIdContext(ctx context.Context, column string) (int64, error)

	// DoneInsertId 执行 INSERT SQL， 同时返回插入记录的 Id
	DoneInsertId(column string) (int64, error)

	// DoneRowsAffectedContext 执行更新 SQL(Update, Delete)， 同时返回操作的记录数
	DoneRowsAffectedContext(ctx context.Context) (int64, error)

	// DoneRowsAffected 执行更新 SQL(Update, Delete)， 同时返回操作的记录数
	DoneRowsAffected() (int64, error)

	// AsSingleContext 执行 SQL，dest 是普通 struct 的引用指针
	AsSingleContext(ctx context.Context, dest any) error

	// AsSingle 执行 SQL，dest 是普通 struct 的引用指针
	AsSingle(dest any) error

	// AsListContext 执行 SQL，dest 是 slice 类型
	AsListContext(ctx context.Context, dest any) error

	// AsList 执行 SQL，dest 是 slice of struct 类型
	AsList(dest any) error

	// AsPrimitiveContext 执行 SQL,dest 是 primitive 类型
	AsPrimitiveContext(ctx context.Context, dest any) error

	// AsPrimitive 执行 SQL,dest 是 primitive 类型
	AsPrimitive(dest any) error

	// AsPrimitiveListContext 执行 SQL,dest 是 slice of primitive 类型
	AsPrimitiveListContext(ctx context.Context, dest any) error

	// AsPrimitiveList 执行 SQL,dest 是 slice of primitive 类型
	AsPrimitiveList(dest any) error

	// AsMapListContext 执行 SQL
	AsMapListContext(ctx context.Context) ([]map[string]any, error)

	// AsMapList 执行 SQL, 结果生成 Map 对象
	AsMapList() ([]map[string]any, error)

	// AsMapContext 执行 SQL
	AsMapContext(ctx context.Context) (map[string]any, error)

	// AsMap 执行 SQL, 结果生成 Map 对象
	AsMap() (map[string]any, error)

	// Reset 重置当前 SqlSession 以再次使用
	Reset() SqlSession

	// LogSql 是否输出 Sql 信息,必须在 SQL 构建执行之前(Done***, As***)调用
	LogSql(logSql bool) SqlSession

	// DbSession SQL 最终代理到 该 DbSession 执行
	DbSession
}

var (
	logSqlEnabled bool
)

func enabledLogSql(enabled bool) {
	logSqlEnabled = enabled
}

type baseSqlSession struct {
	sql       sqltext.SQL
	argMap    map[string]any
	rawSql    []string
	dbSession DbSession
	logSql    bool
}

func newBaseSqlSession(db DbSession) *baseSqlSession {
	return &baseSqlSession{dbSession: db, sql: sqltext.NewSQL(), argMap: map[string]any{}, logSql: logSqlEnabled}
}

func (bss *baseSqlSession) Select(columns ...string) {
	bss.sql.Select(columns...)
}

func (bss *baseSqlSession) From(tables ...string) {
	bss.sql.From(tables...)
}

func (bss *baseSqlSession) Where(condition string, args ...any) {
	bss.sql.Where(condition)
	placeholder := getPlaceholder(condition)
	if len(args) == 0 {
		return
	}
	if len(args) != len(placeholder) {
		panic("the number of SQL parameters and args must be same")
	}
	for index, ph := range placeholder {
		bss.argMap[ph] = args[index]
	}
}

func (bss *baseSqlSession) WhereSelective(condition string, arg any) {
	if isNotZero(arg) {
		bss.sql.Where(condition)
		bss.fillArgValue(condition, arg)
	}
}

func (bss *baseSqlSession) In(column string, args []any) {
	if args == nil || len(args) == 0 {
		return
	}
	b := strings.Builder{}
	b.WriteString(column)
	b.WriteString(" IN (")
	for i := range args {
		if i > 0 {
			b.WriteString(",")
		}
		ph := "#{" + strconv.Itoa(len(bss.argMap)) + "}"
		bss.argMap[ph] = args[i]
		b.WriteString(ph)
	}
	b.WriteString(")")
	bss.sql.Where(b.String())
}

func (bss *baseSqlSession) NotIn(column string, args []any) {
	if args == nil || len(args) == 0 {
		return
	}
	b := strings.Builder{}
	b.WriteString(column)
	b.WriteString(" NOT IN (")
	for i := range args {
		if i > 0 {
			b.WriteString(",")
		}
		ph := "#{" + strconv.Itoa(len(bss.argMap)) + "}"
		bss.argMap[ph] = args[i]
		b.WriteString(ph)
	}
	b.WriteString(")")
	bss.sql.Where(b.String())
}

func (bss *baseSqlSession) GroupBy(columns ...string) {
	bss.sql.GroupBy(columns...)
}

func (bss *baseSqlSession) OrderBy(columns ...string) {
	bss.sql.OrderBy(columns...)
}

func (bss *baseSqlSession) InsertInto(table string) {
	bss.sql.InsertInto(table)
}

func (bss *baseSqlSession) Values(column string, value any) {
	ph := "#{" + column + "}"
	bss.argMap[ph] = value
	bss.sql.Values(column, ph)
}

func (bss *baseSqlSession) ValuesSelective(column string, value any) {
	if isNotZero(value) {
		ph := "#{" + column + "}"
		bss.argMap[ph] = value
		bss.sql.Values(column, "?")
	}
}

func (bss *baseSqlSession) IntoColumns(columns ...string) {
	bss.sql.IntoColumns(columns...)
}

func (bss *baseSqlSession) IntoValues(values ...any) {
	col := make([]string, len(values))
	for i, v := range values {
		ph := "#{" + strconv.Itoa(len(bss.argMap)) + "}"
		col[i] = ph
		bss.argMap[ph] = v
	}
	bss.sql.IntoValues(col...)
}

func (bss *baseSqlSession) IntoMultiValues(values [][]any) {
	if values == nil {
		return
	}
	for index, rowValues := range values {
		col := make([]string, len(rowValues))
		for i, v := range rowValues {
			ph := "#{" + strconv.Itoa(len(bss.argMap)) + "}"
			col[i] = ph
			bss.argMap[ph] = v
		}
		if index > 0 {
			bss.sql.AddRow()
		}
		bss.sql.IntoValues(col...)
	}
}

func (bss *baseSqlSession) Having(condition string, arg any) {
	bss.sql.Having(condition)
	bss.fillArgValue(condition, arg)
}

func (bss *baseSqlSession) Join(joins ...string) {
	bss.sql.Join(joins...)
}

func (bss *baseSqlSession) InnerJoin(joins ...string) {
	bss.sql.InnerJoin(joins...)
}

func (bss *baseSqlSession) InnerJoinSelective(join string, condition any) {
	if isNotZero(condition) {
		bss.sql.InnerJoin(join)
	}
}

func (bss *baseSqlSession) LeftOuterJoin(joins ...string) {
	bss.sql.LeftOuterJoin(joins...)
}

func (bss *baseSqlSession) RightOuterJoin(joins ...string) {
	bss.sql.RightOuterJoin(joins...)
}

func (bss *baseSqlSession) OuterJoin(joins ...string) {
	bss.sql.OuterJoin(joins...)
}

func (bss *baseSqlSession) Or() {
	bss.sql.Or()
}

func (bss *baseSqlSession) And() {
	bss.sql.And()
}

func (bss *baseSqlSession) Limit(limit int) {
	ph := "#{" + strconv.Itoa(len(bss.argMap)) + "}"
	bss.argMap[ph] = limit
	bss.sql.Limit(ph)
}

func (bss *baseSqlSession) Offset(offset int) {
	ph := "#{" + strconv.Itoa(len(bss.argMap)) + "}"
	bss.argMap[ph] = offset
	bss.sql.Offset(ph)
}
func (bss *baseSqlSession) AddParam(param string, value any) {
	bss.argMap[param] = value
}

func (bss *baseSqlSession) Append(sql string, args ...any) {
	bss.rawSql = append(bss.rawSql, sql)
	placeholder := getPlaceholder(sql)
	if len(args) == 0 {
		return
	}
	if len(args) != len(placeholder) {
		panic("the number of SQL parameters and args must be same")
	}
	for index, ph := range placeholder {
		bss.argMap[ph] = args[index]
	}
}

func (bss *baseSqlSession) Update(table string) {
	bss.sql.Update(table)
}

func (bss *baseSqlSession) Set(column string, value any) {
	ph := "#{" + column + "}"
	bss.sql.Set(column + " = " + ph)
	bss.argMap[ph] = value
}

func (bss *baseSqlSession) SetSelective(column string, value any) {
	if isNotZero(value) {
		bss.Set(column, value)
	}
}

func (bss *baseSqlSession) DeleteFrom(table string) {
	bss.sql.DeleteFrom(table)
}

func (bss *baseSqlSession) DoneContext(ctx context.Context, sqlText string, args []any) error {

	if bss.logSql {
		logSql(sqlText, args)
	}
	bss.Reset()
	_, err := bss.dbSession.ExecContext(ctx, sqlText, args...)
	if err != nil {
		return err
	}
	return nil
}

func (bss *baseSqlSession) DoneRowsAffectedContext(ctx context.Context, sqlText string, args []any) (int64, error) {

	if len(args) == 0 {
		return 0, errors.New("必须指定 Where 条件")
	}
	if bss.logSql {
		logSql(sqlText, args)
	}

	bss.Reset()
	result, err := bss.dbSession.ExecContext(ctx, sqlText, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (bss *baseSqlSession) DoneRowsAffected(sqlText string, args []any) (int64, error) {
	return bss.DoneRowsAffectedContext(context.Background(), sqlText, args)
}

func (bss *baseSqlSession) AsSingleContext(ctx context.Context, sqlText string, args []any, dest any) error {

	if dest == nil {
		return fmt.Errorf("scalar value cannot be nil")
	}

	rp := reflect.ValueOf(dest) // 指向存放查询结果的指针。
	if rp.Kind() != reflect.Ptr {
		return fmt.Errorf("dest must be pointer")
	}
	if bss.logSql {
		logSql(sqlText, args)
	}

	bss.Reset()
	rows, err := bss.dbSession.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return err
	}

	defer func(rows *sql.Rows) {
		err = rows.Close()
	}(rows)

	columns, _ := rows.Columns()

	scanDest := getScanDest(rp.Elem(), columns)

	if rows.Next() {
		if err := rows.Scan(scanDest...); err != nil {
			return err
		}
	} else {
		err := rows.Err()
		if err != nil {
			return err
		}
	}
	return err
}

func (bss *baseSqlSession) AsListContext(ctx context.Context, sqlText string, args []any, dest any) error {

	value := reflect.ValueOf(dest) // 指向存放查询结果的切片的指针。
	if value.Kind() != reflect.Ptr {
		return fmt.Errorf("expected pointer to slice of struct, but %T", dest)
	}
	elemValue := value.Elem()       // 存放查询结果的切片。
	elemType := value.Type().Elem() // 存放查询结果的切片的类型。
	if elemType.Kind() != reflect.Slice {
		return fmt.Errorf("eexpected pointer to slice of struct, but %T", elemValue)
	}

	resultType := elemType.Elem() // 存放查询结果的切片的元素的类型。
	sliceContentType := resultType
	if resultType.Kind() == reflect.Struct {
		// 期望映射的结构体的类型。
	} else if resultType.Kind() == reflect.Ptr {
		sliceContentType = resultType.Elem()
		if sliceContentType.Kind() != reflect.Struct {
			return fmt.Errorf("expected slice content is pointer or struct, but %T", sliceContentType)
		}
	} else {
		return fmt.Errorf("expected slice content is pointer or struct, but %T", resultType)
	}

	if bss.logSql {
		logSql(sqlText, args)
	}

	bss.Reset()
	rows, err := bss.dbSession.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return err
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
	}(rows)

	columns, _ := rows.Columns()
	for rows.Next() {
		// 查询结果切片中的一个元素。
		rowDest := reflect.New(sliceContentType).Elem()

		scanDest := getScanDest(rowDest, columns)

		err := rows.Scan(scanDest...)
		if err != nil {
			return err
		}

		// 下面的代码使用反射实现 slice := rawSql(slick, newEle) 的效果。
		if resultType.Kind() == reflect.Struct {
			elemValue.Set(reflect.Append(elemValue, rowDest))
		} else {
			elemValue.Set(reflect.Append(elemValue, rowDest.Addr()))
		}
	}
	return err
}

func getScanDest(rowDest reflect.Value, columns []string) []any {
	scanDest := make([]any, len(columns))
	getDest(rowDest, columns, scanDest)
	for i, dest := range scanDest {
		if dest == nil {
			scanDest[i] = &sql.RawBytes{}
		}
	}
	return scanDest
}

func (bss *baseSqlSession) AsPrimitiveContext(ctx context.Context, sqlText string, args []any, dest any) error {

	if bss.logSql {
		logSql(sqlText, args)
	}

	bss.Reset()
	row := bss.dbSession.QueryRowContext(ctx, sqlText, args...)
	if err := row.Scan(dest); err != nil {
		return err
	}
	return nil
}

func (bss *baseSqlSession) AsPrimitiveListContext(ctx context.Context, sqlText string, args []any, dest any) error {

	if bss.logSql {
		logSql(sqlText, args)
	}
	bss.Reset()
	rows, err := bss.dbSession.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return err
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
	}(rows)

	value := reflect.ValueOf(dest) // 指向存放查询结果的切片的指针。
	if value.Kind() != reflect.Ptr {
		return fmt.Errorf("expected pointer to slice of primitive, but %T", dest)
	}
	elemValue := value.Elem()       // 存放查询结果的切片。
	elemType := value.Type().Elem() // 存放查询结果的切片的类型。
	if elemType.Kind() != reflect.Slice {
		return fmt.Errorf("expected pointer to slice of primitive, but %T", elemValue)
	}

	resultType := elemType.Elem() // 存放查询结果的切片的元素的类型。
	sliceContentType := resultType
	if uint(resultType.Kind()) <= uint(reflect.Float64) {
		// 期望的基本类型。
	} else if resultType.Kind() == reflect.Ptr {
		sliceContentType = resultType.Elem()
		if uint(sliceContentType.Kind()) > uint(reflect.Float64) {
			return fmt.Errorf("expected slice content is pointer or struct, but %T", sliceContentType)
		}
	} else {
		return fmt.Errorf("expected slice content is pointer or primitive, but %T", resultType)
	}

	for rows.Next() {
		// 查询结果切片中的一个元素。
		rowDest := reflect.New(sliceContentType).Elem()
		if err := rows.Scan(rowDest.Addr().Interface()); err != nil {
			return err
		}

		// 下面的代码使用反射实现 slice := rawSql(slick, newEle) 的效果。
		if resultType.Kind() == reflect.Pointer {
			elemValue.Set(reflect.Append(elemValue, rowDest.Addr()))
		} else {
			elemValue.Set(reflect.Append(elemValue, rowDest))
		}
	}

	return err
}

func (bss *baseSqlSession) AsMapListContext(ctx context.Context, sqlText string, args []any) ([]map[string]any, error) {
	if bss.logSql {
		logSql(sqlText, args)
	}
	bss.Reset()
	rows, err := bss.dbSession.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
	}(rows)

	columns, _ := rows.Columns()
	r := make([]map[string]any, 0)
	for rows.Next() {
		results := make([]any, len(columns))
		resultPointers := make([]any, len(columns))
		for i := range columns {
			resultPointers[i] = &results[i]
		}

		if err := rows.Scan(resultPointers...); err != nil {
			return nil, err
		}
		m := make(map[string]any)
		for i, colName := range columns {
			val := resultPointers[i].(*any)
			m[colName] = *val
		}
		r = append(r, m)
	}

	return r, nil
}

func (bss *baseSqlSession) AsMapContext(ctx context.Context, sqlText string, args []any) (map[string]any, error) {

	if bss.logSql {
		logSql(sqlText, args)
	}

	bss.Reset()
	rows, err := bss.dbSession.QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, err
	}

	defer func(rows *sql.Rows) {
		err = rows.Close()
	}(rows)

	columns, _ := rows.Columns()

	results := make([]any, len(columns))
	resultPointers := make([]any, len(columns))
	for i := range columns {
		resultPointers[i] = &results[i]
	}

	if rows.Next() {
		if err := rows.Scan(resultPointers...); err != nil {
			return nil, err
		}
	} else {
		err := rows.Err()
		if err != nil {
			return nil, err
		}
	}
	kvMap := make(map[string]any)
	for i, colName := range columns {
		val := resultPointers[i].(*any)
		kvMap[colName] = *val
	}
	return kvMap, nil
}

func isNotZero(value any) bool {
	switch t := value.(type) {
	case string:
		return value != ""
	case int64:
		return value.(int64) != 0
	case float64:
		return value.(float64) != 0
	case time.Time:
		return !t.IsZero()
	case sql.NullString: // check null stmt types nulls = ''
		return t.Valid
	case sql.NullBool:
		return t.Valid
	case sql.NullInt64:
		return t.Valid
	case sql.NullFloat64:
		return t.Valid
	case int32:
		return value.(int32) != 0
	case int16:
		return value.(int16) != 0
	case int8:
		return value.(int8) != 0
	case int:
		return value.(int) != 0
	case float32:
		return value.(float32) != 0
	default:
		v := reflect.ValueOf(value)
		return !v.IsZero()
	}
}

func (bss *baseSqlSession) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return bss.dbSession.ExecContext(ctx, query, args...)
}

func (bss *baseSqlSession) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return bss.dbSession.PrepareContext(ctx, query)
}

func (bss *baseSqlSession) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return bss.dbSession.QueryRowContext(ctx, query, args...)
}

func (bss *baseSqlSession) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return bss.dbSession.QueryContext(ctx, query, args...)
}

func (bss *baseSqlSession) Exec(query string, args ...any) (sql.Result, error) {
	return bss.dbSession.Exec(query, args...)
}

func (bss *baseSqlSession) Prepare(query string) (*sql.Stmt, error) {
	return bss.dbSession.Prepare(query)
}

func (bss *baseSqlSession) Query(query string, args ...any) (*sql.Rows, error) {
	return bss.dbSession.Query(query, args...)
}

func (bss *baseSqlSession) QueryRow(query string, args ...any) *sql.Row {
	return bss.dbSession.QueryRow(query, args...)
}

func (bss *baseSqlSession) Rollback() error {
	return bss.dbSession.Rollback()
}

func (bss *baseSqlSession) Commit() error {
	return bss.dbSession.Commit()
}

func (bss *baseSqlSession) InTx(txFunc func() error) error {
	return bss.dbSession.InTx(txFunc)
}

func (bss *baseSqlSession) Reset() {
	bss.sql = sqltext.NewSQL()
	bss.argMap = map[string]any{}
	bss.rawSql = nil
	bss.logSql = logSqlEnabled
}

func logSql(sqlText string, args []any) {

	log.Printf("----- SQL -----\n%v", sqlText)
	b := strings.Builder{}
	for i, arg := range args {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%v(%T)", arg, arg))
	}
	log.Printf("----- Parameter -----\n%v", b.String())
}

func getDest(value reflect.Value, columns []string, dest []any) {
	typ := value.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		v := value.Field(i)
		if field.IsExported() {
			if field.Anonymous {
				if v.Type().Kind() == reflect.Pointer {
					f := reflect.New(v.Type().Elem()).Elem()
					v.Set(f.Addr())
					getDest(f, columns, dest)
				} else {
					f := reflect.New(v.Type()).Elem()
					v.Set(f)
					getDest(v, columns, dest)
				}
			} else {
				var fieldName string
				if columnName, ok := field.Tag.Lookup("colname"); ok {
					fieldName = columnName
				} else {
					fieldName = strcase.ToSnake(field.Name)
				}
				for index, name := range columns {
					if name == fieldName {
						if v.Kind() == reflect.Pointer {
							dest[index] = v.Interface()
						} else {
							dest[index] = v.Addr().Interface()
						}
					}
				}
			}
		}
	}
}

func getPlaceholder(s string) []string {
	sIndex := -1
	placeholders := make([]string, 0)
	for i, v := range []byte(s) {
		if (v == '#' || v == '$') && s[i+1] == '{' {
			sIndex = i
		} else if v == '}' && sIndex != -1 {
			placeholders = append(placeholders, s[sIndex:i+1])
			sIndex = -1
		}
	}
	return placeholders
}

func getDynamicAndInjectedPlaceholders(s string) ([]string, []string) {

	dynamicPlaceholders := make([]string, 0)
	injectedPlaceholders := make([]string, 0)
	var dynamic bool
	sIndex := -1
	for i, v := range []byte(s) {
		if v == '#' && s[i+1] == '{' {
			sIndex = i
			dynamic = true
		} else if v == '$' && s[i+1] == '{' {
			sIndex = i
			dynamic = false
		} else if v == '}' && sIndex != -1 {

			if dynamic {
				dynamicPlaceholders = append(dynamicPlaceholders, s[sIndex:i+1])
			} else {
				injectedPlaceholders = append(injectedPlaceholders, s[sIndex:i+1])
			}
			sIndex = -1
		}
	}

	return dynamicPlaceholders, injectedPlaceholders
}

func (bss *baseSqlSession) fillArgValue(sqlText string, value any) {
	placeholder := getPlaceholder(sqlText)
	if len(placeholder) > 0 {
		for _, ph := range placeholder {
			bss.argMap[ph] = value
		}
	}
}
