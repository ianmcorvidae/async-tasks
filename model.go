package main

import (
	"database/sql"
	"github.com/lib/pq"
	"time"
)

type AsyncTaskBehavior struct {
	BehaviorType string                 `json:"type"`
	Data         map[string]interface{} `json:"data"`
}

type AsyncTaskStatus struct {
	Status      string    `json:"status"`
	CreatedDate time.Time `json:"created_date"`
}

type AsyncTask struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Username  string                 `json:"username"`
	Data      map[string]interface{} `json:"data"`
	StartDate *time.Time             `json:"start_date"`
	EndDate   *time.Time             `json:"end_date"`
	Behaviors []AsyncTaskBehavior    `json:"behaviors,omitempty"`
	Status    []AsyncTaskStatus      `json:"status,omitempty"`
}

type DBTask struct {
	ID        string
	Type      string
	Username  sql.NullString
	Data      sql.NullString
	StartDate pq.NullTime
	EndDate   pq.NullTime
}
