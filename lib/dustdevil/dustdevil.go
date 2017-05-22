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
	"log"
	"time"

	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	resty "gopkg.in/resty.v0"
)

// DustDevil forwars received messages to an HTTP endpoint
type DustDevil struct {
	Num      int
	Input    chan []byte
	Shutdown chan struct{}
	Death    chan struct{}
	Config   *erebos.Config
	client   *resty.Client
}

// Start sets up the DustDevil application
func (d *DustDevil) Start() {
	d.client = resty.New()
	d.client = d.client.SetRedirectPolicy(
		resty.FlexibleRedirectPolicy(15)).
		SetDisableWarn(true).
		SetRetryCount(d.Config.DustDevil.RetryCount).
		SetRetryWaitTime(
			time.Duration(d.Config.DustDevil.RetryMinWaitTime)*
				time.Millisecond).
		SetRetryMaxWaitTime(
			time.Duration(d.Config.DustDevil.RetryMaxWaitTime)*
				time.Millisecond).
		SetHeader(`Content-Type`, `application/json`).
		SetContentLength(true)

	d.run()
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
			d.process(msg)
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
func (d *DustDevil) process(msg []byte) {
	var err error

	// unmarshal message
	batch := legacy.MetricBatch{}
	if err = json.Unmarshal(msg, &batch); err != nil {
		log.Println(d.Num, err)
		close(d.Death)
		<-d.Shutdown
		return
	}

	// TODO - convert JSON metrics

	// remove string metrics from the batch
	if d.Config.DustDevil.StripStringMetrics {
		for i := range batch.Data {
			data := batch.Data[i]
			data.StringMetrics = []legacy.StringMetric{}
			batch.Data[i] = data
		}
	}

	if msg, err = batch.MarshalJSON(); err != nil {
		log.Println(d.Num, err)
		close(d.Death)
		<-d.Shutdown
		return
	}

	// timeout must be reset before every request
	r := d.client.SetTimeout(
		time.Duration(d.Config.DustDevil.RequestTimeout) *
			time.Millisecond).
		R()

	// make HTTP POST request
	resp, err := r.SetBody(msg).
		Post(d.Config.DustDevil.Endpoint)
	// check HTTP response
	if err != nil {
		log.Println(d.Num, err)
		// signal main to shut down
		close(d.Death)
		<-d.Shutdown
		return
	}
	if resp.StatusCode() > 299 {
		log.Println(d.Num, resp.Status())
		// signal main to shut down
		close(d.Death)
		<-d.Shutdown
		return
	}
	log.Println(d.Num, resp.Status())
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
