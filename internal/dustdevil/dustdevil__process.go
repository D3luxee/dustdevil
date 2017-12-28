/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package dustdevil // import "github.com/mjolnir42/dustdevil/internal/dustdevil"

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	metrics "github.com/rcrowley/go-metrics"
)

// process is the handler for posting a MetricBatch
func (d *DustDevil) process(msg *erebos.Transport) {
	if msg == nil || msg.Value == nil {
		logrus.Warnf("Ignoring empty message from: %d", msg.HostID)
		if msg != nil {
			d.commit(msg)
		}
		return
	}

	// handle heartbeat messages
	if erebos.IsHeartbeat(msg) {
		d.delay.Go(func() {
			d.lookup.Heartbeat(func() string {
				switch d.Config.Misc.InstanceName {
				case ``:
					return `dustdevil`
				default:
					return fmt.Sprintf("dustdevil/%s",
						d.Config.Misc.InstanceName)
				}
			}(), d.Num, msg.Value)
		})
		return
	}

	// unmarshal message
	var err error
	batch := legacy.MetricBatch{}
	if err = json.Unmarshal(msg.Value, &batch); err != nil {
		// signal main to shut down
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
		// signal main to shut down
		d.Death <- err
		<-d.Shutdown
		return
	}

	// acquire resource limit before issuing the POST request
	d.Limit.Start()

	// timeout must be reset before every request
	r := d.client.SetTimeout(
		time.Duration(d.Config.DustDevil.RequestTimeout) *
			time.Millisecond).
		R()

	// make HTTP POST request
	resp, err := r.SetBody(outMsg).
		Post(d.Config.DustDevil.Endpoint)

	// release resource limit
	d.Limit.Done()

	// check HTTP response
	if err != nil {
		// signal main to shut down
		d.Death <- err
		<-d.Shutdown
		return
	}
	if resp.StatusCode() > 299 {
		// signal main to shut down
		d.Death <- fmt.Errorf("HTTP response was: %s", resp.Status())
		<-d.Shutdown
		return
	}

	metrics.GetOrRegisterMeter(`/output/messages.per.second`,
		*d.Metrics).Mark(1)

	d.delay.Go(func() {
		d.commit(msg)
	})
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
