package statuschangetimeout

import (
	"context"
	"github.com/cyverse-de/async-tasks/database"
	"github.com/cyverse-de/async-tasks/model"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"time"
)

type StatusChangeTimeoutData struct {
	StartStatus string `mapstructure:"start_status"`
	EndStatus   string `mapstructure:"end_status"`
	Timeout     string `mapstructure:"timeout"`
}

func processSingleTask(ctx context.Context, log *logrus.Entry, db *database.DBConnection, ID string) error {
	select {
	// If the context is cancelled, don't bother
	case <-ctx.Done():
		return nil
	default:
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fullTask, err := tx.GetTask(ID)
	if err != nil {
		err = errors.Wrap(err, "failed getting task")
		log.Error(err)
		return err
	}

	var taskData StatusChangeTimeoutData
	for _, behavior := range fullTask.Behaviors {
		// XXX: currently only does the first statuschangetimeout, should do all of them (possibly repeatedly, until none does anything)
		if behavior.BehaviorType == "statuschangetimeout" {
			err := mapstructure.Decode(behavior.Data, &taskData)
			if err != nil {
				// don't die here, let it try other behaviors
				log.Error(errors.Wrap(err, "failed decoding behavior"))
			}
			break
		}
	}

	timeout, err := time.ParseDuration(taskData.Timeout)
	if err != nil {
		err = errors.Wrap(err, "failed parsing timeout duration")
		log.Error(err)
		return err
	}

	var comparisonTimestamp time.Time
	var comparisonStatus string
	if len(fullTask.Statuses) == 0 {
		comparisonTimestamp = *fullTask.StartDate
	} else {
		for _, status := range fullTask.Statuses {
			if status.CreatedDate.After(comparisonTimestamp) {
				comparisonTimestamp = status.CreatedDate
				comparisonStatus = status.Status
			}
		}
	}

	log.Info(comparisonTimestamp)

	if comparisonTimestamp.Add(timeout).Before(time.Now()) && comparisonStatus == taskData.StartStatus {
		newstatus := model.AsyncTaskStatus{Status: taskData.EndStatus}
		err = tx.InsertTaskStatus(newstatus, ID)
		if err != nil {
			err = errors.Wrap(err, "failed inserting task status")
			log.Error(err)
			return err
		}
		tx.Commit()
		log.Infof("updated task given time %s and status %s, timeout/start %s/%s", comparisonTimestamp, comparisonStatus, timeout, taskData.StartStatus)
	} else {
		log.Infof("would NOT update given time %s and status %s, timeout/start %s/%s", comparisonTimestamp, comparisonStatus, timeout, taskData.StartStatus)
	}

	return nil
}

func Processor(ctx context.Context, log *logrus.Entry, _ time.Time, db *database.DBConnection) error {
	filter := database.TaskFilter{
		BehaviorTypes: []string{"statuschangetimeout"},
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tasks, err := tx.GetTasksByFilter(filter)
	if err != nil {
		return err
	}

	tx.Rollback()

	log.Infof("Tasks with statuschangetimeout behavior: %d", len(tasks))

	for _, task := range tasks {
		select {
		// If the context is cancelled, don't bother
		case <-ctx.Done():
			continue
		default:
		}

		err = processSingleTask(ctx, log, db, task.ID)
		if err != nil {
			log.Error(errors.Wrap(err, "failed processing a task"))
		}
	}

	return nil
}
