package main

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"time"
	"fmt"
)

type AsyncTasksApp struct {
	db     *DBConnection
	router *mux.Router
}

func NewAsyncTasksApp(db *DBConnection, router *mux.Router) *AsyncTasksApp {
	app := &AsyncTasksApp{
		db:     db,
		router: router,
	}

	app.InitRoutes()

	return app
}

func (a *AsyncTasksApp) InitRoutes() {
	a.router.NotFoundHandler = http.HandlerFunc(a.NotFound)
	a.router.HandleFunc("/tasks/{id:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", a.GetByIdRequest).Methods("GET").Name("getById")
	a.router.HandleFunc("/tasks", a.GetByFilterRequest).Methods("GET").Name("getByFilter")
	// post new task
	// delete by ID
	// put/patch (?) status, behavior, etc.
}

func (a *AsyncTasksApp) NotFound(writer http.ResponseWriter, r *http.Request) {
	notFound(writer, fmt.Sprintf("no endpoint found at %s %s", r.Method, r.URL.Path))
}

func (a *AsyncTasksApp) GetByIdRequest(writer http.ResponseWriter, r *http.Request) {
	var (
		id string
		ok bool
		v  = mux.Vars(r)
	)

	if id, ok = v["id"]; !ok {
		badRequest(writer, "No ID in URL")
		return
	}

	log.Infof("Fetching async task %s", id)

	tx, err := a.db.BeginTx(context.TODO(), nil)
	if err != nil {
		errored(writer, err.Error())
		return
	}
	defer tx.tx.Rollback()

	task, err := tx.GetTask(id)
	if err != nil {
		errored(writer, err.Error())
		return
	}

	if task.ID == "" {
		notFound(writer, "not found")
		return
	}

	log.Info(task)

	jsoned, err := json.Marshal(task)
	if err != nil {
		errored(writer, err.Error())
		return
	}

	writer.Write(jsoned)

	return
}

func (a *AsyncTasksApp) GetByFilterRequest(writer http.ResponseWriter, r *http.Request) {
	var (
		v = r.URL.Query()

		filters = TaskFilter{
			IDs:       v["id"],
			Types:     v["type"],
			Statuses:  v["status"],
			Usernames: v["username"],
		}
		start_date_since  = v["start_date_since"]
		start_date_before = v["start_date_before"]
		end_date_since    = v["end_date_since"]
		end_date_before   = v["end_date_before"]
		null_end          = v["include_null_end"]
	)

	if len(null_end) > 0 {
		filters.IncludeNullEnd = true
	}

	for _, startdate := range start_date_since {
		parsed, err := time.Parse(time.RFC3339Nano, startdate)
		if err != nil {
			errored(writer, err.Error())
			return
		}
		filters.StartDateSince = append(filters.StartDateSince, parsed)
	}

	for _, startdate := range start_date_before {
		parsed, err := time.Parse(time.RFC3339Nano, startdate)
		if err != nil {
			errored(writer, err.Error())
			return
		}
		filters.StartDateBefore = append(filters.StartDateBefore, parsed)
	}

	for _, enddate := range end_date_since {
		parsed, err := time.Parse(time.RFC3339Nano, enddate)
		if err != nil {
			errored(writer, err.Error())
			return
		}
		filters.EndDateSince = append(filters.EndDateSince, parsed)
	}

	for _, enddate := range end_date_before {
		parsed, err := time.Parse(time.RFC3339Nano, enddate)
		if err != nil {
			errored(writer, err.Error())
			return
		}
		filters.EndDateBefore = append(filters.EndDateBefore, parsed)
	}

	tx, err := a.db.BeginTx(context.TODO(), nil)
	if err != nil {
		errored(writer, err.Error())
		return
	}
	defer tx.tx.Rollback()

	tasks, err := tx.GetTasksByFilter(filters)

	log.Info(tasks)

	jsoned, err := json.Marshal(tasks)
	if err != nil {
		errored(writer, err.Error())
		return
	}

	writer.Write(jsoned)

	return
}

type ErrorResp struct {
	Msg string `json:"msg"`
}

func makeErrorJson(msg string) string {
	err := ErrorResp{Msg: msg}
	jsoned, _ := json.Marshal(err)
	return string(jsoned)
}

func badRequest(writer http.ResponseWriter, msg string) {
	http.Error(writer, makeErrorJson(msg), http.StatusBadRequest)
	log.Error(msg)
}

func errored(writer http.ResponseWriter, msg string) {
	http.Error(writer, makeErrorJson(msg), http.StatusInternalServerError)
	log.Error(msg)
}

func notFound(writer http.ResponseWriter, msg string) {
	http.Error(writer, makeErrorJson(msg), http.StatusNotFound)
	log.Error(msg)
}
