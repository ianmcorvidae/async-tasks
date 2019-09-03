package main

import (
	"context"
	"net/http"
	"encoding/json"
	"github.com/gorilla/mux"
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
	a.router.HandleFunc("/{id:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", app.GetByIdRequest).Methods("GET").Name("getById")
	// list multiple tasks by filter
	// post new task
	// delete by ID
	// put/patch (?) status, behavior, etc.
}

func (a *AsyncTasksApp) GetByIdRequest(writer http.ResponseWriter, r *http.Request) {
	var (
		id  string
		ok  bool
		v   = mux.Vars(r)
	)

	if id, ok = v["id"]; !ok {
		badRequest(writer, "No ID in URL")
		return
	}

	log.Infof("Fetching async task %s", id)

	tx, err := a.db.BeginTx(context.TODO(), nil)
	if err != nil {
		errored(writer, err.Error())
	}
	defer tx.tx.Rollback()

	task, err := tx.GetTask(id)
	if err != nil {
		errored(writer, err.Error())
	}

	log.Info(task)

	jsoned, err := json.Marshal(task)
	if err != nil {
		errored(writer, err.Error())
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

