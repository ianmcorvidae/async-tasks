package main

import (
	"context"
	"time"
	"sync"
)

type BehaviorProcessor func(ctx context.Context, tickerTime time.Time) error

type AsyncTasksUpdater struct {
	db *DBConnection
	behaviorProcessors map[string]BehaviorProcessor
}

func NewAsyncTasksUpdater(db *DBConnection) *AsyncTasksUpdater {
	processors := make(map[string]BehaviorProcessor)

	updater := &AsyncTasksUpdater{
		db: db,
		behaviorProcessors: processors,
	}

	return updater
}

func (u *AsyncTasksUpdater) Do(ctx context.Context, tickerTime time.Time) error {
	log.Infof("Running update with time %s", tickerTime)

	var wg sync.WaitGroup

	wg.Add(1)
	for behaviorType, processor := range u.behaviorProcessors {
		wg.Add(1)
		go func(ctx context.Context, behaviorType string, tickerTime time.Time, wg *sync.WaitGroup) {
			defer wg.Done()
			log.Infof("Processing behavior type %s for time %s", behaviorType, tickerTime)
			err := processor(ctx, tickerTime)
			if err != nil {
				log.Error(err)
			}
		}(ctx, behaviorType, tickerTime, &wg)
	}
	wg.Done()
	wg.Wait()
	log.Infof("Done running update with time %s", tickerTime)
	return nil
}

func (u *AsyncTasksUpdater) AddBehavior(behaviorType string, processor BehaviorProcessor) {
	u.behaviorProcessors[behaviorType] = processor
}
