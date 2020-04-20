package main

import (
	"context"
	"fmt"
	"github.com/cyverse-de/async-tasks/database"
	"github.com/cyverse-de/async-tasks/model"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type BehaviorProcessor func(ctx context.Context, log *logrus.Entry, tickerTime time.Time, db *database.DBConnection) error

type AsyncTasksUpdater struct {
	db                 *database.DBConnection
	behaviorProcessors map[string]BehaviorProcessor
}

func NewAsyncTasksUpdater(db *database.DBConnection) *AsyncTasksUpdater {
	processors := make(map[string]BehaviorProcessor)

	updater := &AsyncTasksUpdater{
		db:                 db,
		behaviorProcessors: processors,
	}

	return updater
}

func createBehaviorProcessorTask(ctx context.Context, behaviorType string, db *database.DBConnection) (string, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	task := model.AsyncTask{Type: fmt.Sprintf("behaviorprocessor-%s", behaviorType)}

	id, err := tx.InsertTask(task)
	if err != nil {
		return "", err
	}

	err = tx.Commit()
	if err != nil {
		return "", err
	}

	return id, nil
}

func checkOldest(ctx context.Context, behaviorType string, db *database.DBConnection, taskID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	filter := database.TaskFilter{
		Types:          []string{fmt.Sprintf("behaviorprocessor-%s", behaviorType)},
		StartDateSince: []time.Time{time.Now().Add(time.Minute * -12)}, // the timeout is 10 minutes, but add some padding
		EndDateSince:   []time.Time{time.Now().AddDate(1, 0, 0)},       // arbitrary point in the future a ways
		IncludeNullEnd: true,
	}

	tasks, err := tx.GetTasksByFilter(filter)
	if err != nil {
		return err
	}

	var t = time.Now()
	var oldestTime = &t
	var isOldest = true
	for _, task := range tasks {
		if oldestTime == nil || oldestTime.IsZero() || task.StartDate.Before(*oldestTime) {
			oldestTime = task.StartDate
			isOldest = (task.ID == taskID)
		}
	}

	if !isOldest {
		return errors.New("The provided ID is not the oldest task of its type")
	}

	return nil
}

func checkAlone(ctx context.Context, behaviorType string, db *database.DBConnection) (string, error) {
	// make a task
	id, err := createBehaviorProcessorTask(ctx, behaviorType, db)
	if err != nil {
		return id, err
	}
	// check that we're the right task to continue
	return id, checkOldest(ctx, behaviorType, db, id)
}

func finishTask(ctx context.Context, taskID string, db *database.DBConnection) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = tx.CompleteTask(taskID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (u *AsyncTasksUpdater) DoPeriodicUpdate(ctx context.Context, tickerTime time.Time, db *database.DBConnection) error {
	log.Infof("Running update with time %s", tickerTime)

	var wg sync.WaitGroup

	wg.Add(1) // add this so there's always at least one thing in the work group
	for behaviorType, processor := range u.behaviorProcessors {
		wg.Add(1)
		go func(ctx context.Context, behaviorType string, processor BehaviorProcessor, tickerTime time.Time, db *database.DBConnection, wg *sync.WaitGroup) {
			defer wg.Done()
			processorLog := log.WithFields(logrus.Fields{
				"behavior_type": behaviorType,
			})
			// check if alone
			taskID, err := checkAlone(ctx, behaviorType, db)
			if taskID != "" {
				defer finishTask(ctx, taskID, db)
			}
			if err != nil {
				processorLog.Error(errors.Wrap(err, "We are not the oldest process for this behavior type"))
				return
			}

			processorLog.Infof("Processing behavior type %s for time %s (task ID %s)", behaviorType, tickerTime, taskID)
			err = processor(ctx, processorLog, tickerTime, db)
			if err != nil {
				processorLog.Error(err)
			}
			processorLog.Infof("Done processing behavior type %s for time %s", behaviorType, tickerTime)
			// release "lock"
		}(ctx, behaviorType, processor, tickerTime, db, &wg)
	}
	wg.Done() // finish our dummy entry in the work group
	wg.Wait()
	log.Infof("Done running update with time %s", tickerTime)
	return nil
}

func (u *AsyncTasksUpdater) AddBehavior(behaviorType string, processor BehaviorProcessor) {
	u.behaviorProcessors[behaviorType] = processor
}
