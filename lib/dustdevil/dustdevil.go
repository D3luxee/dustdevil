/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package dustdevil implements the DustDevil application
package dustdevil

import (
	"encoding/json"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mjolnir42/dustdevil/lib/limit"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	metrics "github.com/rcrowley/go-metrics"
	resty "gopkg.in/resty.v0"
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
	client   *resty.Client
	Metrics  *metrics.Registry
	Limit    *limit.Limit
}

// run is the event loop for DustDevil
func (d *DustDevil) run() {
runloop:
	for {
		select {
		case <-d.Shutdown:
			// drain input channel which will be closed by main
			goto drainloop
		case msg := <-d.Input:
			if msg == nil {
				// we read the closed input channel, skip to read the
				// closed shutdown channel soon...
				continue runloop
			}
			go d.process(msg)
		}
	}
	// compiler: unreachable code

drainloop:
	for {
		select {
		case msg := <-d.Input:
			if msg == nil {
				// closed channel is empty
				break drainloop
			}
			d.process(msg)
		}
	}
}

// process is the handler for posting a MetricBatch
func (d *DustDevil) process(msg *erebos.Transport) {
	var err error
	out := metrics.GetOrRegisterMeter(`/messages`, *d.Metrics)

	// unmarshal message
	batch := legacy.MetricBatch{}
	if err = json.Unmarshal(msg.Value, &batch); err != nil {
		go d.commit(msg)
		d.Death <- err
		<-d.Shutdown
		return
	}

	// remove string metrics from the batch
	if d.Config.DustDevil.StripStringMetrics {
		for i := range batch.Data {
			data := batch.Data[i]
			data.StringMetrics = []legacy.StringMetric{}
			batch.Data[i] = data
		}
	}

	var outMsg []byte
	if outMsg, err = batch.MarshalJSON(); err != nil {
		go d.commit(msg)
		d.Death <- err
		<-d.Shutdown
		return
	}

	// acquire resource limit before issuing the POST request
	d.Limit.Start()
	defer d.Limit.Done()

	// timeout must be reset before every request
	r := d.client.SetTimeout(
		time.Duration(d.Config.DustDevil.RequestTimeout) *
			time.Millisecond).
		R()

	// make HTTP POST request
	resp, err := r.SetBody(outMsg).
		Post(d.Config.DustDevil.Endpoint)
	// check HTTP response
	if err != nil {
		go d.commit(msg)
		// signal main to shut down
		d.Death <- err
		<-d.Shutdown
		return
	}
	if resp.StatusCode() > 299 {
		logrus.Warnf("HTTP response was: %s", resp.Status())
		return
	}
	out.Mark(1)
	go d.commit(msg)
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
