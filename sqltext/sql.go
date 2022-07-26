package sqltext

import "strings"

const (
	and = ") \nAND ("
	or  = ") \nOR ("
)

type SQL interface {
	Select(columns ...string)
	SelectDistinct(columns ...string)
	From(tables ...string)
	Update(table string)
	Set(sets ...string)
	InsertInto(table string)
	Values(columns string, values string)
	IntoColumns(columns ...string)
	IntoValues(values ...string)
	AddRow()
	DeleteFrom(table string)
	Join(joins ...string)
	InnerJoin(joins ...string)
	LeftOuterJoin(joins ...string)
	RightOuterJoin(joins ...string)
	OuterJoin(joins ...string)
	Where(conditions ...string)
	Or()
	And()
	GroupBy(columns ...string)
	Having(conditions ...string)
	OrderBy(columns ...string)
	Limit(limit string)
	Offset(offset string)
	FetchFirstRowsOnly(limit string)
	OffsetRows(offset string)
	String() string
}

// builder 用于构建 sqltext text
type builder struct {
	stmt *Statement
}

func NewSQL() SQL {
	return &builder{stmt: &Statement{values: [][]string{{}}}}
}

func (b *builder) Update(table string) {
	b.stmt.statementType = doUpdate
	b.stmt.tables = append(b.stmt.tables, table)
}

func (b *builder) Set(sets ...string) {
	b.stmt.sets = append(b.stmt.sets, sets...)
}

func (b *builder) InsertInto(table string) {
	b.stmt.statementType = doInsert
	b.stmt.tables = append(b.stmt.tables, table)

}

func (b *builder) Values(columns string, values string) {
	b.IntoColumns(columns)
	b.IntoValues(values)
}

func (b *builder) IntoColumns(columns ...string) {
	b.stmt.columns = append(b.stmt.columns, columns...)
}
func (b *builder) IntoValues(values ...string) {
	list := &b.stmt.values[len(b.stmt.values)-1]
	*list = append(*list, values...)

}

func (b *builder) AddRow() {
	b.stmt.values = append(b.stmt.values, make([]string, 0))
}

func (b *builder) Select(columns ...string) {
	b.stmt.statementType = doSelect
	b.stmt.selects = append(b.stmt.selects, columns...)
}

func (b *builder) SelectDistinct(columns ...string) {
	b.stmt.distinct = true
	b.Select(columns...)
}

func (b *builder) DeleteFrom(table string) {
	b.stmt.statementType = doDelete
	b.stmt.tables = append(b.stmt.tables, table)
}

func (b *builder) From(tables ...string) {
	b.stmt.tables = append(b.stmt.tables, tables...)
}

func (b *builder) Join(joins ...string) {
	b.stmt.join = append(b.stmt.join, joins...)
}

func (b *builder) InnerJoin(joins ...string) {
	b.stmt.innerJoin = append(b.stmt.innerJoin, joins...)
}

func (b *builder) LeftOuterJoin(joins ...string) {
	b.stmt.leftOuterJoin = append(b.stmt.leftOuterJoin, joins...)
}

func (b *builder) RightOuterJoin(joins ...string) {
	b.stmt.rightOuterJoin = append(b.stmt.rightOuterJoin, joins...)

}

func (b *builder) OuterJoin(joins ...string) {
	b.stmt.outerJoin = append(b.stmt.outerJoin, joins...)
}

func (b *builder) Where(conditions ...string) {
	b.stmt.where = append(b.stmt.where, conditions...)
	b.stmt.lastList = &b.stmt.where
}

func (b *builder) Or() {
	*b.stmt.lastList = append(*b.stmt.lastList, or)

}

func (b *builder) And() {
	*b.stmt.lastList = append(*b.stmt.lastList, and)
}

func (b *builder) GroupBy(columns ...string) {
	b.stmt.groupBy = append(b.stmt.groupBy, columns...)
}

func (b *builder) Having(conditions ...string) {
	b.stmt.having = append(b.stmt.having, conditions...)
	b.stmt.lastList = &b.stmt.having

}

func (b *builder) OrderBy(columns ...string) {
	b.stmt.orderBy = append(b.stmt.orderBy, columns...)
}

func (b *builder) Limit(limit string) {
	b.stmt.limit = limit
	b.stmt.limitingRowsStrategy = OffsetLimit
}

func (b *builder) Offset(offset string) {
	b.stmt.offset = offset
	b.stmt.limitingRowsStrategy = OffsetLimit
}

func (b *builder) FetchFirstRowsOnly(limit string) {
	b.stmt.limit = limit
	b.stmt.limitingRowsStrategy = Iso
}

func (b *builder) OffsetRows(offset string) {
	b.stmt.offset = offset
	b.stmt.limitingRowsStrategy = Iso
}

func (b *builder) String() string {
	builder := &strings.Builder{}
	b.stmt.sql(builder)
	return builder.String()
}

type statementType int

const (
	doDelete statementType = iota
	doInsert
	doSelect
	doUpdate
)

type Statement struct {
	statementType        statementType
	sets                 []string
	selects              []string
	tables               []string
	join                 []string
	innerJoin            []string
	outerJoin            []string
	leftOuterJoin        []string
	rightOuterJoin       []string
	where                []string
	having               []string
	groupBy              []string
	orderBy              []string
	lastList             *[]string
	columns              []string
	values               [][]string
	distinct             bool
	offset               string
	limit                string
	limitingRowsStrategy limitingRowsStrategy
}

func (s *Statement) sql(builder *strings.Builder) {

	switch s.statementType {
	case doSelect:
		s.selectSql(builder)
	case doDelete:
		s.deleteSql(builder)
	case doInsert:
		s.insertSql(builder)
	case doUpdate:
		s.updateSql(builder)
	default:

	}
}
func (s *Statement) selectSql(builder *strings.Builder) {
	if s.distinct {
		s.sqlClause(builder, "SELECT DISTINCT", s.selects, "", "", ", ")
	} else {
		s.sqlClause(builder, "SELECT", s.selects, "", "", ", ")
	}
	s.sqlClause(builder, "FROM", s.tables, "", "", ", ")
	s.joins(builder)
	s.sqlClause(builder, "WHERE", s.where, "(", ")", " AND ")
	s.sqlClause(builder, "GROUP BY", s.groupBy, "", "", ", ")
	s.sqlClause(builder, "HAVING", s.having, "(", ")", " AND ")
	s.sqlClause(builder, "ORDER BY", s.orderBy, "", "", ", ")
	s.limitingRowsStrategy.appendClause(builder, s.offset, s.limit)
}

func (s *Statement) deleteSql(builder *strings.Builder) {
	s.sqlClause(builder, "DELETE FROM", s.tables, "", "", "")
	s.sqlClause(builder, "WHERE", s.where, "(", ")", " AND ")
	s.limitingRowsStrategy.appendClause(builder, "", s.limit)
}

func (s *Statement) insertSql(builder *strings.Builder) {
	s.sqlClause(builder, "INSERT INTO", s.tables, "", "", "")
	s.sqlClause(builder, "", s.columns, "(", ")", ", ")
	for i, value := range s.values {
		var keyword = "VALUES"
		if i > 0 {
			keyword = ","
		}
		s.sqlClause(builder, keyword, value, "(", ")", ", ")
	}
}

func (s *Statement) updateSql(builder *strings.Builder) {
	s.sqlClause(builder, "UPDATE", s.tables, "", "", "")
	s.joins(builder)
	s.sqlClause(builder, "SET", s.sets, "", "", ", ")
	s.sqlClause(builder, "WHERE", s.where, "(", ")", " AND ")
	s.limitingRowsStrategy.appendClause(builder, "", s.limit)
}

func (s *Statement) sqlClause(builder *strings.Builder, keyword string, parts []string, open string, close string, conjunction string) {
	if len(parts) > 0 {
		if builder.Len() > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(keyword)
		builder.WriteString(" ")
		builder.WriteString(open)
		last := "________"
		for i, part := range parts {
			if i > 0 && part != and && part != or && last != and && last != or {
				builder.WriteString(conjunction)
			}
			builder.WriteString(part)
			last = part
		}
		builder.WriteString(close)
	}
}

func (s *Statement) joins(builder *strings.Builder) {
	s.sqlClause(builder, "JOIN", s.join, "", "", "\nJOIN ")
	s.sqlClause(builder, "INNER JOIN", s.innerJoin, "", "", "\nINNER JOIN ")
	s.sqlClause(builder, "OUTER JOIN", s.outerJoin, "", "", "\nOUTER JOIN ")
	s.sqlClause(builder, "LEFT OUTER JOIN", s.leftOuterJoin, "", "", "\nLEFT OUTER JOIN ")
	s.sqlClause(builder, "RIGHT OUTER JOIN", s.rightOuterJoin, "", "", "\nRIGHT OUTER JOIN ")
}

type limitingRowsStrategy int

const (
	Nop limitingRowsStrategy = iota
	Iso
	OffsetLimit
)

func (ls limitingRowsStrategy) appendClause(builder *strings.Builder, offset string, limit string) {
	switch ls {
	case OffsetLimit:
		if limit != "" {
			builder.WriteString(" LIMIT ")
			builder.WriteString(limit)
		}
		if offset != "" {
			builder.WriteString(" OFFSET ")
			builder.WriteString(offset)
		}
	case Iso:
		if offset != "" {
			builder.WriteString(" OFFSET ")
			builder.WriteString(offset)
			builder.WriteString(" ROWS")
		}
		if limit != "" {
			builder.WriteString(" FETCH FIRST ")
			builder.WriteString(limit)
			builder.WriteString(" ROWS ONLY")
		}
	case Nop:
	default:
		// 啥也不做
	}
}
