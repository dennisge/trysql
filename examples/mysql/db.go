// Package tidb 该模块定义了 TIDB 数据库 初始化的方法和公共对象。

package mysql

import (
	"context"
	"database/sql"
	"github.com/dennisge/trysql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	tidb *sql.DB
	ssf  trysql.SqlSessionFactory
)

// InitDB 初始化数据配置。
// dsn 数据库连接字符串。
// 尝试根据指定的连接字符串创建数据库连接并且Ping，如果成功则返回nil，否则返回连接时发生的错误。
func InitDB(dsn string, maxActive, maxIdle, maxLifeSeconds int, logSql bool) error {

	if db_, err := sql.Open("mysql", dsn); err != nil {
		return err
	} else {
		db_.SetMaxOpenConns(maxActive) // 100
		db_.SetMaxIdleConns(maxIdle)
		db_.SetConnMaxLifetime(time.Duration(maxLifeSeconds) * time.Second)
		db_.SetConnMaxIdleTime(time.Duration(maxLifeSeconds) * time.Second)

		if err := db_.Ping(); err != nil {
			return err
		} else {
			tidb = db_
			ssf = trysql.NewSqlSessionFactory(trysql.Mysql, tidb, 300*time.Second, logSql)
			return nil
		}
	}
}

// DoContext 调用 sqlHandler
func DoContext(ctx context.Context, sqlHandler trysql.SqlHandler) error {
	return ssf.DoContext(ctx, sqlHandler)
}

// Do 调用 sqlHandler
func Do(sqlHandler trysql.SqlHandler) error {
	return ssf.DoContext(context.TODO(), sqlHandler)
}

// DoInTxContext 在事务中 调用 sqlHandler
func DoInTxContext(ctx context.Context, sqlHandler trysql.SqlHandler) error {
	return ssf.DoInTxContext(ctx, sqlHandler)

}

// DoInTx 在事务中 调用 sqlHandler
func DoInTx(sqlHandler trysql.SqlHandler) error {
	return ssf.DoInTxContext(context.TODO(), sqlHandler)
}
