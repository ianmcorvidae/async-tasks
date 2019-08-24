package main

import (
	"net/http"
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

	app.router.HandleFunc("/{id:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", app.GetByIdRequest).Methods("GET").Name("getById")
	// list multiple tasks by filter
	// post new task
	// delete by ID
	// put/patch (?) status, behavior, etc.

	return app
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

	return
}

func badRequest(writer http.ResponseWriter, msg string) {
        http.Error(writer, msg, http.StatusBadRequest)
        log.Error(msg)
}

