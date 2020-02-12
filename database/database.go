package database

import (
	"context"

	"database/sql"
	"github.com/cyverse-de/async-tasks/model"
	"github.com/cyverse-de/dbutil"
	"github.com/lib/pq"

	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"strings"
	"time"

	"encoding/json"
)

// DBConnection wraps a sql.DB
type DBConnection struct {
	db  *sql.DB
	log *logrus.Entry
}

// DBTx wraps a sql.Tx for this DB
type DBTx struct {
	tx  *sql.Tx
	log *logrus.Entry
}

// SetupDB initializes a DBConnection for the given dbURI
func SetupDB(dbURI string, log *logrus.Entry) (*DBConnection, error) {
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

	return &DBConnection{db: db, log: log}, nil
}

// Close defers to sql.DB Close()
func (d *DBConnection) Close() error {
	return d.db.Close()
}

// GetCount gets a count of async tasks in the DB
func (d *DBConnection) GetCount() (int64, error) {
	row := d.db.QueryRow("SELECT COUNT(*) FROM async_tasks")
	var res struct{ count int64 }
	err := row.Scan(&res.count)
	if err != nil {
		return 0, err
	}
	return res.count, nil
}

// BeginTx starts a DBTx for the given DBConnection
func (d *DBConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*DBTx, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &DBTx{tx: tx, log: d.log}, nil
}

// Rollback defers to underlying Rollback
func (t *DBTx) Rollback() error {
	return t.tx.Rollback()
}

// Commit defers to underlying Commit
func (t *DBTx) Commit() error {
	return t.tx.Commit()
}

// GetBaseTask fetches a task from the database by ID (sans behaviors/statuses)
func (t *DBTx) GetBaseTask(id string) (*model.AsyncTask, error) {
	query := `SELECT id, type, username, data,
	                 start_date at time zone (select current_setting('TIMEZONE')) AS start_date,
			 end_date at time zone (select current_setting('TIMEZONE')) AS end_date
	            FROM async_tasks WHERE id::text = $1`

	rows, err := t.tx.Query(query, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dbtask model.DBTask
	for rows.Next() {
		if err := rows.Scan(&dbtask.ID, &dbtask.Type, &dbtask.Username, &dbtask.Data, &dbtask.StartDate, &dbtask.EndDate); err != nil {
			return nil, err
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return makeTask(dbtask)
}

func makeTask(dbtask model.DBTask) (*model.AsyncTask, error) {
	var err error
	task := &model.AsyncTask{ID: dbtask.ID, Type: dbtask.Type}

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

// DeleteTask deletes a task from the database by ID
func (t *DBTx) DeleteTask(id string) error {
	query := `DELETE FROM async_tasks WHERE id::text = $1`

	_, err := t.tx.Exec(query, id)
	if err != nil {
		return err
	}

	return nil
}

// GetTask fetches a task from the database by ID, including behaviors and statuses
func (t *DBTx) GetTask(id string) (*model.AsyncTask, error) {
	task, err := t.GetBaseTask(id)
	if err != nil {
		return task, err
	}

	behaviors, err := t.GetTaskBehaviors(id)
	if err != nil {
		return task, err
	}
	task.Behaviors = behaviors

	statuses, err := t.GetTaskStatuses(id)
	if err != nil {
		return task, err
	}
	task.Statuses = statuses

	return task, err
}

// GetTaskBehaviors fetches a task's set of behaviors from the DB by ID
func (t *DBTx) GetTaskBehaviors(id string) ([]model.AsyncTaskBehavior, error) {
	query := `SELECT behavior_type, data FROM async_task_behavior WHERE async_task_id::text = $1`

	rows, err := t.tx.Query(query, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var behaviors []model.AsyncTaskBehavior
	for rows.Next() {
		var dbbehavior model.DBTaskBehavior
		if err := rows.Scan(&dbbehavior.BehaviorType, &dbbehavior.Data); err != nil {
			return nil, err
		}

		behavior := model.AsyncTaskBehavior{BehaviorType: dbbehavior.BehaviorType}
		if dbbehavior.Data.Valid {
			jsonData := make(map[string]interface{})

			err = json.Unmarshal([]byte(dbbehavior.Data.String), &jsonData)
			if err != nil {
				return behaviors, err
			}

			behavior.Data = jsonData
		}

		behaviors = append(behaviors, behavior)
	}

	return behaviors, nil
}

// GetTaskStatuses fetches a tasks's list of statuses from the DB by ID, ordered by creation date
func (t *DBTx) GetTaskStatuses(id string) ([]model.AsyncTaskStatus, error) {
	query := `SELECT status, created_date at time zone (select current_setting('TIMEZONE')) AS created_date
	            FROM async_task_status WHERE async_task_id::text = $1 ORDER BY created_date ASC`

	rows, err := t.tx.Query(query, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statuses []model.AsyncTaskStatus
	for rows.Next() {
		var status model.AsyncTaskStatus
		if err := rows.Scan(&status.Status, &status.CreatedDate); err != nil {
			return nil, err
		}

		statuses = append(statuses, status)
	}

	return statuses, nil
}

type TaskFilter struct {
	IDs             []string
	Types           []string
	Usernames       []string
	StartDateSince  []time.Time
	StartDateBefore []time.Time
	EndDateSince    []time.Time
	EndDateBefore   []time.Time
	IncludeNullEnd  bool
	Statuses        []string
}

// GetTasksByFilter fetches a set of tasks by a set of provided filters
func (t *DBTx) GetTasksByFilter(filters TaskFilter) ([]model.AsyncTask, error) {
	t.log.Info(filters)

	var tasks []model.AsyncTask
	var args []interface{}
	var wheres []string

	query := `SELECT id, type, username, data,
	                 start_date at time zone (select current_setting('TIMEZONE')) AS start_date,
			 end_date at time zone (select current_setting('TIMEZONE')) AS end_date
	            FROM async_tasks`

	currentIndex := 1

	if len(filters.IDs) > 0 {
		wheres = append(wheres, fmt.Sprintf(" id::text = ANY($%d)", currentIndex))
		args = append(args, pq.Array(filters.IDs))
		currentIndex = currentIndex + 1
	}

	if len(filters.Types) > 0 {
		wheres = append(wheres, fmt.Sprintf(" type = ANY($%d)", currentIndex))
		args = append(args, pq.Array(filters.Types))
		currentIndex = currentIndex + 1
	}

	if len(filters.Usernames) > 0 {
		wheres = append(wheres, fmt.Sprintf(" username = ANY($%d)", currentIndex))
		args = append(args, pq.Array(filters.Usernames))
		currentIndex = currentIndex + 1
	}

	if len(filters.StartDateSince) > 0 {
		if len(filters.StartDateSince) > 1 {
			t.log.Warn("More than one start_date_since filter is unsupported. Only the oldest date will be considered.")
		}
		wheres = append(wheres, fmt.Sprintf(" start_date > ANY($%d)", currentIndex))
		args = append(args, pq.Array(filters.StartDateSince))
		currentIndex = currentIndex + 1
	}

	if len(filters.StartDateBefore) > 0 {
		if len(filters.StartDateBefore) > 1 {
			t.log.Warn("More than one start_date_before filter is unsupported. Only the newest date will be considered.")
		}
		wheres = append(wheres, fmt.Sprintf(" start_date < ANY($%d)", currentIndex))
		args = append(args, pq.Array(filters.StartDateBefore))
		currentIndex = currentIndex + 1
	}

	if len(filters.EndDateSince) > 0 {
		if len(filters.EndDateSince) > 1 {
			t.log.Warn("More than one end_date_since filter is unsupported. Only the oldest date will be considered.")
		}
		if filters.IncludeNullEnd {
			wheres = append(wheres, fmt.Sprintf(" (end_date > ANY($%d) OR end_date IS NULL)", currentIndex))
		} else {
			wheres = append(wheres, fmt.Sprintf(" end_date > ANY($%d)", currentIndex))
		}
		args = append(args, pq.Array(filters.EndDateSince))
		currentIndex = currentIndex + 1
	}

	if len(filters.EndDateBefore) > 0 {
		if len(filters.EndDateBefore) > 1 {
			t.log.Warn("More than one end_date_before filter is unsupported. Only the newest date will be considered.")
		}
		wheres = append(wheres, fmt.Sprintf(" end_date < ANY($%d)", currentIndex))
		args = append(args, pq.Array(filters.EndDateBefore))
		currentIndex = currentIndex + 1
	}

	if len(filters.Statuses) > 0 {
		query = query + " JOIN async_task_status ON (async_task_status.async_task_id = async_tasks.id AND async_task_status.created_date = (select max(created_date) FROM async_task_status WHERE async_task_id = async_tasks.id))"
		wheres = append(wheres, fmt.Sprintf(" status = ANY($%d)", currentIndex))
		args = append(args, pq.Array(filters.Statuses))
		currentIndex = currentIndex + 1
	}

	if len(wheres) > 0 {
		query = query + " WHERE " + strings.Join(wheres, " AND ")
	}

	rows, err := t.tx.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var dbtask model.DBTask
		if err := rows.Scan(&dbtask.ID, &dbtask.Type, &dbtask.Username, &dbtask.Data, &dbtask.StartDate, &dbtask.EndDate); err != nil {
			return nil, err
		}

		task, err := makeTask(dbtask)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, *task)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}

// InsertTask inserts a provided AsyncTask into the DB and returns the task's generated ID as a string
func (t *DBTx) InsertTask(task model.AsyncTask) (string, error) {
	var columns []string
	var placeholders []string
	var args []interface{}

	currentIndex := 1

	if task.Type == "" {
		return "", errors.New("Task type must be provided")
	}
	columns = append(columns, "type")
	placeholders = append(placeholders, fmt.Sprintf("$%d", currentIndex))
	args = append(args, task.Type)
	currentIndex = currentIndex + 1

	if task.Username != "" {
		columns = append(columns, "username")
		placeholders = append(placeholders, fmt.Sprintf("$%d", currentIndex))
		args = append(args, task.Username)
		currentIndex = currentIndex + 1
	}

	if len(task.Data) > 0 {
		jsoned, err := json.Marshal(task.Data)
		if err != nil {
			return "", err
		}

		columns = append(columns, "data")
		placeholders = append(placeholders, fmt.Sprintf("$%d", currentIndex))
		args = append(args, jsoned)
		currentIndex = currentIndex + 1
	}

	if task.StartDate == nil || task.StartDate.IsZero() {
		columns = append(columns, "start_date")
		placeholders = append(placeholders, "now()")
	} else {
		columns = append(columns, "start_date")
		placeholders = append(placeholders, fmt.Sprintf("$%d AT TIME ZONE (select current_setting('TIMEZONE'))", currentIndex))
		args = append(args, task.StartDate)
		currentIndex = currentIndex + 1
	}

	if task.EndDate != nil && !task.EndDate.IsZero() {
		columns = append(columns, "end_date")
		placeholders = append(placeholders, fmt.Sprintf("$%d AT TIME ZONE (select current_setting('TIMEZONE'))", currentIndex))
		args = append(args, task.EndDate)
		currentIndex = currentIndex + 1
	}

	query := fmt.Sprintf(`INSERT INTO async_tasks (%s) VALUES (%s) RETURNING id::text`, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	rows, err := t.tx.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var id string
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return "", err
		}
	}

	if err = rows.Err(); err != nil {
		return "", err
	}

	// This will only insert one status, we're assuming only one gets to this point (enforced in the route, not here)
	if len(task.Statuses) > 0 {
		err = t.InsertTaskStatus(task.Statuses[0], id)
		if err != nil {
			return "", err
		}
	}

	// this could end up poorly performant if we want to insert a bunch at once
	for _, behavior := range task.Behaviors {
		err = t.InsertTaskBehavior(behavior, id)
		if err != nil {
			return "", err
		}
	}

	return id, nil
}

// InsertTaskStatus inserts a provided AsyncTaskStatus into the DB for the provided async task ID
func (t *DBTx) InsertTaskStatus(status model.AsyncTaskStatus, taskID string) error {
	var query string
	var args []interface{}

	if status.Status == "" {
		return errors.New("Status type must be provided")
	}

	args = append(args, taskID)
	args = append(args, status.Status)
	if status.CreatedDate.IsZero() {
		query = `INSERT INTO async_task_status (async_task_id, status, created_date) VALUES ($1, $2, now())`
	} else {
		query = `INSERT INTO async_task_status (async_task_id, status, created_date) VALUES ($1, $2, $3 AT TIME ZONE (select current_setting('TIMEZONE')))`
		args = append(args, status.CreatedDate)
	}

	rows, err := t.tx.Query(query, args...)
	if err != nil {
		return err
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}

	return nil
}

// InsertTaskBehavior inserts a provided AsyncTaskBehavior into the DB for the provided async task ID
func (t *DBTx) InsertTaskBehavior(behavior model.AsyncTaskBehavior, taskID string) error {
	var query string
	var args []interface{}

	if behavior.BehaviorType == "" {
		return errors.New("Behavior type must be provided")
	}

	args = append(args, taskID)
	args = append(args, behavior.BehaviorType)

	if len(behavior.Data) > 0 {
		query = `INSERT INTO async_task_behavior (async_task_id, behavior_type, data) VALUES ($1, $2, $3)`
		jsoned, err := json.Marshal(behavior.Data)
		if err != nil {
			return err
		}
		args = append(args, jsoned)
	} else {
		query = `INSERT INTO async_task_behavior (async_task_id, behavior_type) VALUES ($1, $2)`
	}

	rows, err := t.tx.Query(query, args...)
	if err != nil {
		return err
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}

	return nil
}
