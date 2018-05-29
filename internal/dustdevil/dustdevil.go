/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package dustdevil implements the DustDevil metric forwarder
package dustdevil // import "github.com/mjolnir42/dustdevil/internal/dustdevil"

import (
	"sync"
	"time"

	"github.com/go-resty/resty"
	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	wall "github.com/mjolnir42/eye/lib/eye.wall"
	"github.com/mjolnir42/legacy"
	"github.com/mjolnir42/limit"
	metrics "github.com/rcrowley/go-metrics"
)

// Handlers is the registry of running application handlers
var Handlers map[int]erebos.Handler

func init() {
	Handlers = make(map[int]erebos.Handler)
}

// DustDevil forwars received messages to an HTTP endpoint
type DustDevil struct {
	Num      int
	Input    chan *erebos.Transport
	Shutdown chan struct{}
	Death    chan error
	Config   *erebos.Config
	Metrics  *metrics.Registry
	Limit    *limit.Limit
	// unexported
	client         *resty.Client
	delay          *delay.Delay
	lookup         *wall.Lookup
	assembly       map[int]map[time.Time]legacy.MetricData
	assemblyLock   sync.Mutex
	assemblyCommit map[int][]*erebos.Transport
}

// commit marks a message as fully processed
func (d *DustDevil) commit(msg *erebos.Transport) {
	msg.Commit <- &erebos.Commit{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
