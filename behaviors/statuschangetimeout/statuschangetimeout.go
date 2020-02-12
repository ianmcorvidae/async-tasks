package statuschangetimeout

import (
	"context"
	"github.com/cyverse-de/async-tasks/database"
	"github.com/sirupsen/logrus"
	"time"
)

func Processor(_ context.Context, log *logrus.Entry, _ time.Time, db *database.DBConnection) error {
	c, err := db.GetCount()
	if err != nil {
		return err
	}
	log.Infof("There are %d tasks in the DB", c)
	return nil
}
