package main

import (
	"context"

	"database/sql"
	"github.com/cyverse-de/dbutil"

	_ "github.com/lib/pq"
)

// DBConnection wraps a sql.DB
type DBConnection struct {
	db *sql.DB
}

// DBTx wraps a sql.Tx for this DB
type DBTx struct {
	tx *sql.Tx
}

// SetupDB initializes a DBConnection for the given dbURI
func SetupDB(dbURI string) (*DBConnection, error) {
	log.Info("Connecting to the database...")

	connector, err := dbutil.NewDefaultConnector("1m")
	if err != nil {
		return nil, err
	}

	db, err := connector.Connect("postgres", dbURI)
	if err != nil {
		return nil, err
	}

	log.Info("Connected to the database")

	if err = db.Ping(); err != nil {
		return nil, err
	}

	log.Info("Successfully pinged the database")

	return &DBConnection{db: db}, nil
}

// BeginTx starts a DBTx for the given DBConnection
func (d *DBConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*DBTx, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &DBTx{tx: tx}, nil
}
