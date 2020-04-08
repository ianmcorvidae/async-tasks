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
	Complete    bool   `mapstructure:"complete"`
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

	fullTask, err := tx.GetTask(ID, true)
	if err != nil {
		err = errors.Wrap(err, "failed getting task")
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

	log.Infof("Most recent timestamp for task: %s", comparisonTimestamp)

	for _, behavior := range fullTask.Behaviors {
		// only one of each type because of the DB FK
		if behavior.BehaviorType == "statuschangetimeout" {
			data, ok := behavior.Data["statuses"].([]interface{})
			if !ok {
				err = errors.New("Behavior data is not an array")
				log.Error(err)
				return err
			}
			for _, datum := range data {
				var taskData StatusChangeTimeoutData
				err := mapstructure.Decode(datum, &taskData)
				if err != nil {
					// don't die here, let it try other behaviors
					log.Error(errors.Wrap(err, "failed decoding behavior"))
					continue
				}

				timeout, err := time.ParseDuration(taskData.Timeout)
				if err != nil {
					// don't die here, let it try other behaviors
					log.Error(errors.Wrap(err, "failed parsing timeout duration"))
					continue
				}

				if comparisonTimestamp.Add(timeout).Before(time.Now()) && comparisonStatus == taskData.StartStatus {
					newstatus := model.AsyncTaskStatus{Status: taskData.EndStatus}
					err = tx.InsertTaskStatus(newstatus, ID)
					if err != nil {
						// do die here, because the transaction is probably dead
						err = errors.Wrap(err, "failed inserting task status")
						log.Error(err)
						return err
					}
					if taskData.Complete {
						err = tx.CompleteTask(ID)
						if err != nil {
							// do die here, because the transaction is probably dead
							err = errors.Wrap(err, "failed inserting task status")
							log.Error(err)
							return err
						}
					}
					log.Infof("Updated task with time %s and timeout %s from '%s' to '%s', set complete: %t", comparisonTimestamp, timeout, comparisonStatus, taskData.EndStatus, taskData.Complete)
				} else {
					log.Infof("Task was not ready to update given time %s, timeout %s, and status '%s'", comparisonTimestamp, timeout, comparisonStatus)
				}
			}
		}
	}

	tx.Commit()

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
