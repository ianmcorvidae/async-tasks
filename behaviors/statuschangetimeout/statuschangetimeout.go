package statuschangetimeout

import (
	"context"
	"github.com/cyverse-de/async-tasks/database"
	"github.com/cyverse-de/async-tasks/model"
	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
	"github.com/pkg/errors"
	"time"
)

type StatusChangeTimeoutData struct {
	StartStatus string `mapstructure:"start_status"`
	EndStatus   string `mapstructure:"end_status"`
	Timeout     string `mapstructure:"timeout"`
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

	log.Infof("Tasks with statuschangetimeout behavior: %d", len(tasks))

	for _, task := range tasks {
		// rollback here before creating a new tx below, whatever tx is set to now
		tx.Rollback()

		select {
		// If the context is cancelled, don't bother
		case <-ctx.Done():
			continue
		default:
		}

		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		fullTask, err := tx.GetTask(task.ID)
		if err != nil {
			log.Error(errors.Wrap(err, "failed getting task"))
			continue
		}

		var taskData StatusChangeTimeoutData
		for _, behavior := range fullTask.Behaviors {
			if behavior.BehaviorType == "statuschangetimeout" {
				err := mapstructure.Decode(behavior.Data, &taskData)
				if err != nil {
					log.Error(errors.Wrap(err, "failed decoding behavior"))
				}
				break
			}
		}

		timeout, err := time.ParseDuration(taskData.Timeout)
		if err != nil {
			log.Error(errors.Wrap(err, "failed parsing timeout duration"))
			continue
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
			err = tx.InsertTaskStatus(newstatus, task.ID)
			if err != nil {
				log.Error(errors.Wrap(err, "failed inserting task status"))
				continue
			}
			tx.Commit()
			log.Infof("updated task given time %s and status %s, timeout/start %s/%s", comparisonTimestamp, comparisonStatus, timeout, taskData.StartStatus)
		} else {
			log.Infof("would NOT update given time %s and status %s, timeout/start %s/%s", comparisonTimestamp, comparisonStatus, timeout, taskData.StartStatus)
		}
	}

	// just in case
	tx.Rollback()

	return nil
}
