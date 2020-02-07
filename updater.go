package main

import (
	"time"
)

type AsyncTasksUpdater struct {
	db *DBConnection
}

func NewAsyncTasksUpdater(db *DBConnection) *AsyncTasksUpdater {
	updater := &AsyncTasksUpdater{
		db: db,
	}

	return updater
}

func (u *AsyncTasksUpdater) Do(tickerTime time.Time) error {
	log.Infof("Running update with time %s", tickerTime)
	return nil
}
