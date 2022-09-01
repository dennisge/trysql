package trysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type DbType uint8

const (
	Mysql DbType = iota
	Postgresql
)

// SqlHandler sql 执行函数 ， ctx 是 Timeout Context
type SqlHandler func(ctx context.Context, sqlSession SqlSession) error

type SqlSessionFactory interface {

	// NewSqlSession 新建一个 非事务 SqlSession
	NewSqlSession() SqlSession

	// NewTxSqlSession 新建一个 事务性 SqlSession, dbSession 提供事务支持
	NewTxSqlSession(dbSession DbSession) SqlSession

	// NewTxDbSession 新建一个 DbSession
	NewTxDbSession() DbSession

	// NewTxDbSessionContext 新建一个 DbSession
	//
	// The provided context is used until the transaction is committed or rolled back.
	// If the context is canceled, the sql package will roll back
	// the transaction. Tx.Commit will return an error if the context provided to
	// BeginTx is canceled.
	//
	// The provided TxOptions is optional and may be nil if defaults should be used.
	// If a non-default isolation level is used that the driver doesn't support,
	// an error will be returned.
	NewTxDbSessionContext(ctx context.Context, opts *sql.TxOptions) DbSession

	// NewTimeoutContext 新建一个 Timeout Context
	NewTimeoutContext(ctx context.Context, duration ...time.Duration) (context.Context, context.CancelFunc)

	// DoTimeoutContext 执行 非事务 Sql 查询，指定超时时间
	DoTimeoutContext(timeout time.Duration, ctx context.Context, sqlHandler SqlHandler) error

	// DoContext 执行 非事务 Sql 查询，使用默认超时
	DoContext(ctx context.Context, sqlHandler SqlHandler) error

	// Do 执行 非事务 Sql 查询， 使用默认超时
	Do(sqlHandler SqlHandler) error

	// DoInTxTimeoutContext 在一个事务中 执行 Sql 查询，指定超时时间
	DoInTxTimeoutContext(timeout time.Duration, ctx context.Context, sqlHandler SqlHandler) error

	// DoInTxContext 在一个事务中 执行 Sql 查询，使用默认超时
	DoInTxContext(ctx context.Context, sqlHandler SqlHandler) error

	// DoInTx 在一个事务中 执行 Sql 查询，使用默认超时
	DoInTx(sqlHandler SqlHandler) error
}
type DefaultSqlSessionFactory struct {
	dbType         DbType
	db             *sql.DB
	nonTxDbSession DbSession
	sqlTimeout     time.Duration
}

// NewSqlSessionFactory 新建一个 SqlSessionFactory，sqlTimeout 指定一个 SqlSession的 执行超时时间
func NewSqlSessionFactory(dbType DbType, db *sql.DB, sqlTimeout time.Duration, logSqlEnabled bool) SqlSessionFactory {
	var ssf = &DefaultSqlSessionFactory{}
	ssf.db = db
	session := NewTxSession(db, false)
	ssf.nonTxDbSession = session
	ssf.dbType = dbType
	ssf.sqlTimeout = sqlTimeout
	enabledLogSql(logSqlEnabled)
	return ssf
}

// NewSqlSessionFactoryByDSN 初始化数据配置。
// dsn 数据库连接字符串。
// 尝试根据指定的连接字符串创建数据库连接并且Ping，如果成功则返回nil，否则返回连接时发生的错误。
func NewSqlSessionFactoryByDSN(dbType DbType, dsn string, maxActive, maxIdle int, connMaxLifetime, connMaxIdleTime, sqlTimeout time.Duration, logSqlEnabled bool) (SqlSessionFactory, error) {
	var driverName string
	if dbType == Mysql {
		driverName = "mysql"
	} else if dbType == Postgresql {
		driverName = "postgres"
	} else {
		return nil, fmt.Errorf("not supported db type %v", dbType)
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	if maxActive > 0 {
		db.SetMaxOpenConns(maxActive)
	}
	if maxIdle > 0 {
		db.SetMaxIdleConns(maxIdle)
	}
	if connMaxLifetime > 0 {
		db.SetConnMaxLifetime(connMaxLifetime)
	}
	if connMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(connMaxIdleTime)
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return NewSqlSessionFactory(dbType, db, sqlTimeout, logSqlEnabled), nil
}

func (ssf *DefaultSqlSessionFactory) NewSqlSession() SqlSession {
	switch ssf.dbType {
	case Postgresql:
		return NewPostgreSqlSession(ssf.nonTxDbSession)
	case Mysql:
		return NewMySqlSession(ssf.nonTxDbSession)
	default:
		// 不会执行到此
		return nil
	}
}

func (ssf *DefaultSqlSessionFactory) NewTxDbSession() DbSession {
	return ssf.NewTxDbSessionContext(context.Background(), nil)
}

func (ssf *DefaultSqlSessionFactory) NewTimeoutContext(ctx context.Context, duration ...time.Duration) (context.Context, context.CancelFunc) {
	if len(duration) > 0 {
		return context.WithTimeout(ctx, duration[0])
	}
	return context.WithTimeout(ctx, ssf.sqlTimeout)
}

func (ssf *DefaultSqlSessionFactory) NewTxDbSessionContext(ctx context.Context, opts *sql.TxOptions) DbSession {
	return NewTxSessionContext(ssf.db, true, ctx, opts)
}

func (ssf *DefaultSqlSessionFactory) NewTxSqlSession(dbSession DbSession) SqlSession {
	switch ssf.dbType {
	case Postgresql:
		return NewPostgreSqlSession(dbSession)
	case Mysql:
		return NewMySqlSession(dbSession)
	default:
		// 不会执行到此
		return nil
	}
}

func (ssf *DefaultSqlSessionFactory) DoTimeoutContext(timeout time.Duration, ctx context.Context, sqlHandler SqlHandler) error {
	timeoutContext, cancelFunc := ssf.NewTimeoutContext(ctx, timeout)
	defer cancelFunc()
	sqlSession := ssf.NewSqlSession()
	return sqlHandler(timeoutContext, sqlSession)
}

func (ssf *DefaultSqlSessionFactory) DoContext(ctx context.Context, sqlHandler SqlHandler) error {
	return ssf.DoTimeoutContext(ssf.sqlTimeout, ctx, sqlHandler)
}

func (ssf *DefaultSqlSessionFactory) Do(sqlHandler SqlHandler) error {
	return ssf.DoTimeoutContext(ssf.sqlTimeout, context.TODO(), sqlHandler)
}

func (ssf *DefaultSqlSessionFactory) DoInTxTimeoutContext(timeout time.Duration, ctx context.Context, sqlHandler SqlHandler) error {
	timeoutContext, cancelFunc := ssf.NewTimeoutContext(ctx, timeout)
	defer cancelFunc()
	dbSession := ssf.NewTxDbSessionContext(timeoutContext, nil)
	sqlSession := ssf.NewTxSqlSession(dbSession)
	return dbSession.InTx(func() error {
		return sqlHandler(timeoutContext, sqlSession)
	})
}

func (ssf *DefaultSqlSessionFactory) DoInTxContext(ctx context.Context, sqlHandler SqlHandler) error {
	return ssf.DoInTxTimeoutContext(ssf.sqlTimeout, ctx, sqlHandler)
}

func (ssf *DefaultSqlSessionFactory) DoInTx(sqlHandler SqlHandler) error {
	return ssf.DoInTxTimeoutContext(ssf.sqlTimeout, context.TODO(), sqlHandler)
}
