package trysql

import (
	"context"
	"database/sql"
	"log"
)

// DbSession (SQL Go database connection) is a wrapper for SQL database handler ( can be *sql.DB or *sql.Tx)
// It should be able to work with all SQL data that follows SQL standard.
type DbSession interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Prepare(query string) (*sql.Stmt, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)

	// Rollback a transaction
	Rollback() error
	// Commit a transaction
	Commit() error
	// InTx commits a transaction if no errors, otherwise rollback
	// txFunc is the operations wrapped in a transaction
	InTx(txFunc func() error) error
}

// NonTxDbSession is the concrete implementation of DbSession by using *sql.DB
type NonTxDbSession struct {
	DB *sql.DB
}

// TxDbSession is the concrete implementation of DbSession by using *sql.Tx
type TxDbSession struct {
	DB *sql.Tx
}

func (tx *NonTxDbSession) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return tx.DB.ExecContext(ctx, query, args...)
}

func (tx *NonTxDbSession) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.DB.Exec(query, args...)
}

func (tx *NonTxDbSession) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return tx.DB.PrepareContext(ctx, query)
}

func (tx *NonTxDbSession) Prepare(query string) (*sql.Stmt, error) {
	return tx.DB.Prepare(query)
}

func (tx *NonTxDbSession) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.DB.QueryContext(ctx, query, args...)
}

func (tx *NonTxDbSession) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.DB.Query(query, args...)
}

func (tx *NonTxDbSession) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.DB.QueryRowContext(ctx, query, args...)
}

func (tx *NonTxDbSession) QueryRow(query string, args ...interface{}) *sql.Row {
	return tx.DB.QueryRow(query, args...)
}

// Rollback NOP
func (tx *NonTxDbSession) Rollback() error {
	return nil
}

//Commit NOP
func (tx *NonTxDbSession) Commit() error {
	return nil
}

// InTx NOP
func (tx *NonTxDbSession) InTx(_ func() error) error {
	return nil
}

func (tx *TxDbSession) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return tx.DB.ExecContext(ctx, query, args...)
}

func (tx *TxDbSession) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.DB.Exec(query, args...)
}

func (tx *TxDbSession) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return tx.DB.PrepareContext(ctx, query)
}

func (tx *TxDbSession) Prepare(query string) (*sql.Stmt, error) {
	return tx.DB.Prepare(query)
}

func (tx *TxDbSession) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.DB.QueryContext(ctx, query, args...)
}

func (tx *TxDbSession) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.DB.Query(query, args...)
}

func (tx *TxDbSession) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.DB.QueryRowContext(ctx, query, args...)
}

func (tx *TxDbSession) QueryRow(query string, args ...interface{}) *sql.Row {
	return tx.DB.QueryRow(query, args...)
}

func (tx *TxDbSession) InTx(txFunc func() error) error {
	var err error
	tdb := tx.DB
	defer func() {
		if p := recover(); p != nil {
			log.Println("found panic and rollback:", p)
			err := tdb.Rollback()
			if err != nil {
				panic(err)
			}
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			log.Println("found error and rollback:", err)
			err = tdb.Rollback() // if Rollback returns error update err with commit err
		} else {
			log.Println("commit")
			err = tdb.Commit() // if Commit returns error update err with commit err
		}
	}()
	err = txFunc()
	return err
}

func (tx *TxDbSession) Rollback() error {
	return tx.DB.Rollback()
}

func (tx *TxDbSession) Commit() error {
	return tx.DB.Commit()
}

// NewTxSession  创建 DbSession ,tx 为 true 时， 开启事务
func NewTxSession(sdb *sql.DB, tx bool) DbSession {
	return NewTxSessionContext(sdb, tx, context.Background(), nil)
}

// NewTxSessionContext 创建 DbSession ,tx 为 true 时， 开启事务
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back
// the transaction. Tx.Commit will return an error if the context provided to
// BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func NewTxSessionContext(sdb *sql.DB, tx bool, ctx context.Context, opts *sql.TxOptions) DbSession {
	var sdt DbSession
	if tx {
		tx, err := sdb.BeginTx(ctx, opts)
		if err != nil {
			panic(err)
		}
		sdt = &TxDbSession{DB: tx}
	} else {
		sdt = &NonTxDbSession{DB: sdb}
	}
	return sdt
}
