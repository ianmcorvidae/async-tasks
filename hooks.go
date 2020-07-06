package main

import (
	"context"
	"github.com/cyverse-de/async-tasks/database"
	"github.com/sirupsen/logrus"
)

type HookProcessor func(ctx context.Context, log *logrus.Entry, db *database.DBConnection, event interface{})

type HooksManager struct {
	db    *database.DBConnection
	hooks map[string][]HookProcessor
}

func NewHooksManager(db *database.DBConnection) *HooksManager {
	hooks := make(map[string][]HookProcessor)

	manager := &HooksManager{
		db:    db,
		hooks: hooks,
	}

	return manager
}

func (m *HooksManager) Add(hookType string, handler HookProcessor) {
	m.hooks[hookType] = append(m.hooks[hookType], handler)
}

func (m *HooksManager) Run(ctx context.Context, hookType string, event interface{}) {
	hookLog := log.WithFields(logrus.Fields{
		"hook_type": hookType,
	})
	hookLog.Infof("Handling hook: %s", hookType)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, processor := range m.hooks[hookType] {
		select {
		// if the context is cancelled, no-op
		case <-ctx.Done():
			continue
		default:
		}

		processor(ctx, hookLog.WithFields(logrus.Fields{"hook_index": i}), m.db, event)
	}
}
