package trysql

import (
	"context"
	"strconv"
	"strings"
)

type PostgreSqlSession struct {
	*baseSqlSession
}

func NewPostgreSqlSession(dbSession DbSession) SqlSession {
	sqlBuilder := newBaseSqlSession(dbSession)
	return &PostgreSqlSession{sqlBuilder}
}

func (sb *PostgreSqlSession) Select(columns ...string) SqlSession {
	sb.baseSqlSession.Select(columns...)
	return sb
}

func (sb *PostgreSqlSession) From(tables ...string) SqlSession {
	sb.baseSqlSession.From(tables...)
	return sb
}

func (sb *PostgreSqlSession) Where(condition string, args ...any) SqlSession {
	sb.baseSqlSession.Where(condition, args...)
	return sb
}

func (sb *PostgreSqlSession) WhereSelective(condition string, arg any) SqlSession {
	sb.baseSqlSession.WhereSelective(condition, arg)
	return sb
}

func (sb *PostgreSqlSession) In(column string, args []any) SqlSession {
	sb.baseSqlSession.In(column, args)
	return sb
}

func (sb *PostgreSqlSession) NotIn(column string, args []any) SqlSession {
	sb.baseSqlSession.NotIn(column, args)
	return sb
}

func (sb *PostgreSqlSession) GroupBy(columns ...string) SqlSession {
	sb.baseSqlSession.GroupBy(columns...)
	return sb
}

func (sb *PostgreSqlSession) Having(condition string, value any) SqlSession {
	sb.baseSqlSession.Having(condition, value)
	return sb
}

func (sb *PostgreSqlSession) OrderBy(columns ...string) SqlSession {
	sb.baseSqlSession.OrderBy(columns...)
	return sb
}

func (sb *PostgreSqlSession) InsertInto(table string) SqlSession {
	sb.baseSqlSession.InsertInto(table)
	return sb
}

func (sb *PostgreSqlSession) Values(column string, value any) SqlSession {
	sb.baseSqlSession.Values(column, value)
	return sb
}

func (sb *PostgreSqlSession) ValuesSelective(column string, value any) SqlSession {
	sb.baseSqlSession.ValuesSelective(column, value)
	return sb
}

func (sb *PostgreSqlSession) IntoColumns(columns ...string) SqlSession {
	sb.baseSqlSession.IntoColumns(columns...)
	return sb
}

func (sb *PostgreSqlSession) IntoValues(values ...any) SqlSession {
	sb.baseSqlSession.IntoValues(values...)
	return sb
}

func (sb *PostgreSqlSession) IntoMultiValues(values [][]any) SqlSession {
	sb.baseSqlSession.IntoMultiValues(values)
	return sb
}

func (sb *PostgreSqlSession) Update(table string) SqlSession {
	sb.baseSqlSession.Update(table)
	return sb
}

func (sb *PostgreSqlSession) Set(column string, value any) SqlSession {
	sb.baseSqlSession.Set(column, value)
	return sb
}

func (sb *PostgreSqlSession) SetSelective(column string, value any) SqlSession {
	sb.baseSqlSession.SetSelective(column, value)
	return sb
}

func (sb *PostgreSqlSession) DeleteFrom(table string) SqlSession {
	sb.baseSqlSession.DeleteFrom(table)
	return sb
}

func (sb *PostgreSqlSession) Join(joins ...string) SqlSession {
	sb.baseSqlSession.Join(joins...)
	return sb
}

func (sb *PostgreSqlSession) InnerJoin(joins ...string) SqlSession {
	sb.baseSqlSession.InnerJoin(joins...)
	return sb
}

func (sb *PostgreSqlSession) InnerJoinSelective(join string, condition any) SqlSession {
	sb.baseSqlSession.InnerJoinSelective(join, condition)
	return sb
}

func (sb *PostgreSqlSession) LeftOuterJoin(joins ...string) SqlSession {
	sb.baseSqlSession.LeftOuterJoin(joins...)
	return sb
}

func (sb *PostgreSqlSession) RightOuterJoin(joins ...string) SqlSession {
	sb.baseSqlSession.RightOuterJoin(joins...)
	return sb
}

func (sb *PostgreSqlSession) OuterJoin(joins ...string) SqlSession {
	sb.baseSqlSession.OuterJoin(joins...)
	return sb
}

func (sb *PostgreSqlSession) Or() SqlSession {
	sb.baseSqlSession.Or()
	return sb
}

func (sb *PostgreSqlSession) And() SqlSession {
	sb.baseSqlSession.And()
	return sb
}

func (sb *PostgreSqlSession) Limit(limit int) SqlSession {
	sb.baseSqlSession.Limit(limit)
	return sb
}

func (sb *PostgreSqlSession) Offset(offset int) SqlSession {
	sb.baseSqlSession.Offset(offset)
	return sb
}

func (sb *PostgreSqlSession) AddParam(param string, value any) SqlSession {
	sb.baseSqlSession.AddParam(param, value)
	return sb
}
func (sb *PostgreSqlSession) AppendRaw(sql string, args ...any) SqlSession {
	sb.baseSqlSession.Append(sql, args...)
	return sb
}

func (sb *PostgreSqlSession) DoneContext(ctx context.Context) error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.DoneContext(ctx, sqlText, args)
}

func (sb *PostgreSqlSession) Done() error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.Done(sqlText, args)
}

func (sb *PostgreSqlSession) DoneInsertIdContext(ctx context.Context, column string) (int64, error) {
	sqlText, args := sb.builderSQLText()
	sqlText += "\n RETURNING " + column
	if sb.logSql {
		logSql(sqlText, args)
	}
	var id int64
	err := sb.baseSqlSession.dbSession.QueryRowContext(ctx, sqlText, args...).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (sb *PostgreSqlSession) DoneInsertId(column string) (int64, error) {
	return sb.DoneInsertIdContext(context.Background(), column)
}

func (sb *PostgreSqlSession) DoneRowsAffectedContext(ctx context.Context) (int64, error) {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.DoneRowsAffectedContext(ctx, sqlText, args)
}

func (sb *PostgreSqlSession) DoneRowsAffected() (int64, error) {
	return sb.DoneRowsAffectedContext(context.Background())
}

func (sb *PostgreSqlSession) AsSingleContext(ctx context.Context, dest any) error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.AsSingleContext(ctx, sqlText, args, dest)
}

func (sb *PostgreSqlSession) AsSingle(dest any) error {
	return sb.AsSingleContext(context.Background(), dest)
}

func (sb *PostgreSqlSession) AsListContext(ctx context.Context, dest any) error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.AsListContext(ctx, sqlText, args, dest)
}

func (sb *PostgreSqlSession) AsList(dest any) error {
	return sb.AsListContext(context.Background(), dest)
}

func (sb *PostgreSqlSession) AsPrimitiveContext(ctx context.Context, dest any) error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.AsPrimitiveContext(ctx, sqlText, args, dest)
}
func (sb *PostgreSqlSession) AsPrimitive(dest any) error {
	return sb.AsPrimitiveContext(context.Background(), dest)
}
func (sb *PostgreSqlSession) AsPrimitiveListContext(ctx context.Context, dest any) error {
	sqlText, args := sb.builderSQLText()
	return sb.baseSqlSession.AsPrimitiveListContext(ctx, sqlText, args, dest)
}
func (sb *PostgreSqlSession) AsPrimitiveList(dest any) error {
	return sb.AsPrimitiveListContext(context.Background(), dest)
}

func (sb *PostgreSqlSession) InTx(txFunc func() error) error {
	return sb.InTx(txFunc)
}

func (sb *PostgreSqlSession) Reset() SqlSession {
	sb.baseSqlSession.Reset()
	return sb
}

func (sb *PostgreSqlSession) LogSql(logSql bool) SqlSession {
	sb.baseSqlSession.logSql = logSql
	return sb
}

func (sb *PostgreSqlSession) builderSQLText() (string, []any) {
	var sqlText = sb.sql.String() + " " + strings.Join(sb.rawSql, " ")
	placeholder := getPlaceholder(sqlText)
	args := make([]any, 0)
	if len(placeholder) > 0 {
		for index, value := range placeholder {
			sqlText = strings.Replace(sqlText, value, "$"+strconv.Itoa(index+1), 1)
			args = append(args, sb.argMap[value])
		}
	}

	return sqlText, args
}
