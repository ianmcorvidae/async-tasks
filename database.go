package main

import (
	"context"

	"database/sql"
	"github.com/cyverse-de/dbutil"

	"encoding/json"

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

	log.Info("Created database connector")

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

// GetTask fetches a task from the database by ID
func (t *DBTx) GetTask(id string) (*AsyncTask, error) {

	query := `SELECT id, type, username, data, start_date, end_date FROM async_tasks WHERE id::text = $1`

	rows, err := t.tx.Query(query, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dbtask DBTask
	for rows.Next() {
		if err := rows.Scan(&dbtask.ID, &dbtask.Type, &dbtask.Username, &dbtask.Data, &dbtask.StartDate, &dbtask.EndDate); err != nil {
			return nil, err
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	log.Info(dbtask)

	task := &AsyncTask{ID: dbtask.ID, Type: dbtask.Type}

	if dbtask.Username.Valid {
		task.Username = dbtask.Username.String
	}

	if dbtask.Data.Valid {
		jsonData := make(map[string]interface{})

		err = json.Unmarshal([]byte(dbtask.Data.String), &jsonData)
		if err != nil {
			return task, err
		}

		task.Data = jsonData
	}

	if dbtask.StartDate.Valid {
		task.StartDate = &dbtask.StartDate.Time
	}

	if dbtask.EndDate.Valid {
		task.EndDate = &dbtask.EndDate.Time
	}

	return task, nil
}
