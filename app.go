package main

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
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
	a.router.HandleFunc("/{id:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", a.GetByIdRequest).Methods("GET").Name("getById")
	a.router.HandleFunc("/", a.GetByFilterRequest).Methods("GET").Name("getByFilter")
	// list multiple tasks by filter
	// post new task
	// delete by ID
	// put/patch (?) status, behavior, etc.
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
			IDs: v["id"],
			Types: v["type"],
			Statuses: v["status"],
			Usernames: v["username"],
		}
		//start_date_since  = v["start_date_since"]
		//start_date_before = v["start_date_before"]
		//end_date_since    = v["end_date_since"]
		//end_date_before   = v["end_date_before"]
	)

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

func badRequest(writer http.ResponseWriter, msg string) {
	http.Error(writer, msg, http.StatusBadRequest)
	log.Error(msg)
}

func errored(writer http.ResponseWriter, msg string) {
	http.Error(writer, msg, http.StatusInternalServerError)
	log.Error(msg)
}

func notFound(writer http.ResponseWriter, msg string) {
	http.Error(writer, msg, http.StatusNotFound)
	log.Error(msg)
}
