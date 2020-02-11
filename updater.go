package main

import (
	"context"
	"sync"
	"time"
)

type BehaviorProcessor func(ctx context.Context, tickerTime time.Time) error

type AsyncTasksUpdater struct {
	db                 *DBConnection
	behaviorProcessors map[string]BehaviorProcessor
}

func NewAsyncTasksUpdater(db *DBConnection) *AsyncTasksUpdater {
	processors := make(map[string]BehaviorProcessor)

	updater := &AsyncTasksUpdater{
		db:                 db,
		behaviorProcessors: processors,
	}

	return updater
}

func (u *AsyncTasksUpdater) DoPeriodicUpdate(ctx context.Context, tickerTime time.Time) error {
	log.Infof("Running update with time %s", tickerTime)

	var wg sync.WaitGroup

	wg.Add(1) // add this so there's always at least one thing in the work group
	for behaviorType, processor := range u.behaviorProcessors {
		wg.Add(1)
		go func(ctx context.Context, behaviorType string, processor BehaviorProcessor, tickerTime time.Time, wg *sync.WaitGroup) {
			defer wg.Done()
			log.Infof("Processing behavior type %s for time %s", behaviorType, tickerTime)
			err := processor(ctx, tickerTime)
			if err != nil {
				log.Error(err)
			}
			log.Infof("Done processing behavior type %s for time %s", behaviorType, tickerTime)
		}(ctx, behaviorType, processor, tickerTime, &wg)
	}
	wg.Done() // finish our dummy entry in the work group
	wg.Wait()
	log.Infof("Done running update with time %s", tickerTime)
	return nil
}

func (u *AsyncTasksUpdater) AddBehavior(behaviorType string, processor BehaviorProcessor) {
	u.behaviorProcessors[behaviorType] = processor
}
