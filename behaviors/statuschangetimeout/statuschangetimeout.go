package statuschangetimeout

import (
	"context"
	"github.com/cyverse-de/async-tasks/database"
	"github.com/sirupsen/logrus"
	"time"
)

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

	log.Infof("Tasks with statuschangetimeout behavior: %s", tasks)
	return nil
}
