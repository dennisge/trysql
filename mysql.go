package trysql

import (
	"context"
	"strings"
)

type MySqlSession struct {
	*baseSqlSession
}

func NewMySqlSession(dbSession DbSession) SqlSession {
	sqlBuilder := newBaseSqlSession(dbSession)
	return &MySqlSession{sqlBuilder}
}
func (sb *MySqlSession) Select(columns ...string) SqlSession {
	sb.baseSqlSession.Select(columns...)
	return sb
}

func (sb *MySqlSession) From(tables ...string) SqlSession {
	sb.baseSqlSession.From(tables...)
	return sb
}

func (sb *MySqlSession) Where(condition string, args ...any) SqlSession {
	sb.baseSqlSession.Where(condition, args...)
	return sb
}

func (sb *MySqlSession) WhereSelective(condition string, arg any) SqlSession {
	sb.baseSqlSession.WhereSelective(condition, arg)
	return sb
}

func (sb *MySqlSession) In(column string, args []any) SqlSession {
	sb.baseSqlSession.In(column, args)
	return sb
}

func (sb *MySqlSession) NotIn(column string, args []any) SqlSession {
	sb.baseSqlSession.NotIn(column, args)
	return sb
}

func (sb *MySqlSession) GroupBy(columns ...string) SqlSession {
	sb.baseSqlSession.GroupBy(columns...)
	return sb
}

func (sb *MySqlSession) Having(condition string, value any) SqlSession {
	sb.baseSqlSession.Having(condition, value)
	return sb
}

func (sb *MySqlSession) OrderBy(columns ...string) SqlSession {
	sb.baseSqlSession.OrderBy(columns...)
	return sb
}

func (sb *MySqlSession) InsertInto(table string) SqlSession {
	sb.baseSqlSession.InsertInto(table)
	return sb
}

func (sb *MySqlSession) Values(column string, value any) SqlSession {
	sb.baseSqlSession.Values(column, value)
	return sb
}

func (sb *MySqlSession) ValuesSelective(column string, value any) SqlSession {
	sb.baseSqlSession.ValuesSelective(column, value)
	return sb
}

func (sb *MySqlSession) IntoColumns(columns ...string) SqlSession {
	sb.baseSqlSession.IntoColumns(columns...)
	return sb
}

func (sb *MySqlSession) IntoValues(values ...any) SqlSession {
	sb.baseSqlSession.IntoValues(values...)
	return sb
}

func (sb *MySqlSession) IntoMultiValues(values [][]any) SqlSession {
	sb.baseSqlSession.IntoMultiValues(values)
	return sb
}

func (sb *MySqlSession) Update(table string) SqlSession {
	sb.baseSqlSession.Update(table)
	return sb
}

func (sb *MySqlSession) Set(column string, value any) SqlSession {
	sb.baseSqlSession.Set(column, value)
	return sb
}

func (sb *MySqlSession) SetSelective(column string, value any) SqlSession {
	sb.baseSqlSession.SetSelective(column, value)
	return sb
}

func (sb *MySqlSession) DeleteFrom(table string) SqlSession {
	sb.baseSqlSession.DeleteFrom(table)
	return sb
}

func (sb *MySqlSession) Join(joins ...string) SqlSession {
	sb.baseSqlSession.Join(joins...)
	return sb
}

func (sb *MySqlSession) InnerJoin(joins ...string) SqlSession {
	sb.baseSqlSession.InnerJoin(joins...)
	return sb
}

func (sb *MySqlSession) InnerJoinSelective(join string, condition any) SqlSession {
	sb.baseSqlSession.InnerJoinSelective(join, condition)
	return sb
}

func (sb *MySqlSession) LeftOuterJoin(joins ...string) SqlSession {
	sb.baseSqlSession.LeftOuterJoin(joins...)
	return sb
}

func (sb *MySqlSession) RightOuterJoin(joins ...string) SqlSession {
	sb.baseSqlSession.RightOuterJoin(joins...)
	return sb
}

func (sb *MySqlSession) OuterJoin(joins ...string) SqlSession {
	sb.baseSqlSession.OuterJoin(joins...)
	return sb
}

func (sb *MySqlSession) Or() SqlSession {
	sb.baseSqlSession.Or()
	return sb
}

func (sb *MySqlSession) And() SqlSession {
	sb.baseSqlSession.And()
	return sb
}

func (sb *MySqlSession) Limit(limit int) SqlSession {
	sb.baseSqlSession.Limit(limit)
	return sb
}

func (sb *MySqlSession) Offset(offset int) SqlSession {
	sb.baseSqlSession.Offset(offset)
	return sb
}

func (sb *MySqlSession) AddParam(param string, value any) SqlSession {
	sb.baseSqlSession.AddParam(param, value)
	return sb
}
func (sb *MySqlSession) AppendRaw(sql string, args ...any) SqlSession {
	sb.baseSqlSession.Append(sql, args...)
	return sb
}

func (sb *MySqlSession) DoneContext(ctx context.Context) error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.DoneContext(ctx, sqlText, args)
}

func (sb *MySqlSession) Done() error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.Done(sqlText, args)
}

func (sb *MySqlSession) DoneInsertIdContext(ctx context.Context, _ string) (int64, error) {
	sqlText, args := sb.builderSQLText()
	if sb.logSql {
		logSql(sqlText, args)
	}
	result, err := sb.baseSqlSession.ExecContext(ctx, sqlText, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (sb *MySqlSession) DoneInsertId(column string) (int64, error) {
	return sb.DoneInsertIdContext(context.Background(), column)
}

func (sb *MySqlSession) DoneRowsAffectedContext(ctx context.Context) (int64, error) {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.DoneRowsAffectedContext(ctx, sqlText, args)
}

func (sb *MySqlSession) DoneRowsAffected() (int64, error) {
	return sb.DoneRowsAffectedContext(context.Background())
}

func (sb *MySqlSession) AsSingleContext(ctx context.Context, dest any) error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.AsSingleContext(ctx, sqlText, args, dest)
}

func (sb *MySqlSession) AsSingle(dest any) error {
	return sb.AsSingleContext(context.Background(), dest)
}

func (sb *MySqlSession) AsListContext(ctx context.Context, dest any) error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.AsListContext(ctx, sqlText, args, dest)
}

func (sb *MySqlSession) AsList(dest any) error {
	return sb.AsListContext(context.Background(), dest)
}

func (sb *MySqlSession) AsPrimitiveContext(ctx context.Context, dest any) error {

	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.AsPrimitiveContext(ctx, sqlText, args, dest)
}
func (sb *MySqlSession) AsPrimitive(dest any) error {
	return sb.AsPrimitiveContext(context.Background(), dest)
}
func (sb *MySqlSession) AsPrimitiveListContext(ctx context.Context, dest any) error {

	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.AsPrimitiveListContext(ctx, sqlText, args, dest)
}
func (sb *MySqlSession) AsPrimitiveList(dest any) error {
	return sb.AsPrimitiveListContext(context.Background(), dest)
}

func (sb *MySqlSession) Reset() SqlSession {
	sb.baseSqlSession.Reset()
	return sb
}

func (sb *MySqlSession) LogSql(logSql bool) SqlSession {
	sb.baseSqlSession.logSql = logSql
	return sb
}

func (sb *MySqlSession) builderSQLText() (string, []any) {
	var sqlText = sb.sql.String() + " " + strings.Join(sb.rawSql, " ")
	dynamicPlaceholders, injectedPlaceholders := getDynamicAndInjectedPlaceholders(sqlText)
	args := make([]any, 0)

	for _, value := range dynamicPlaceholders {
		sqlText = strings.Replace(sqlText, value, "?", 1)
		args = append(args, sb.argMap[value])
	}

	for _, value := range injectedPlaceholders {
		injected := sb.argMap[value]
		sqlText = strings.Replace(sqlText, value, injected.(string), 1)
	}

	return sqlText, args
}
