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

	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	metrics "github.com/rcrowley/go-metrics"
)

// processElastic is the handler for posting a MetricBatch to
// Elasticsearch
func (d *DustDevil) processElastic(msg *erebos.Transport) {
	var err error
	out := metrics.GetOrRegisterMeter(`/output/messages.per.second`, *d.Metrics)

	// unmarshal message
	batch := legacy.MetricBatch{}
	if err = json.Unmarshal(msg.Value, &batch); err != nil {
		d.Death <- err
		<-d.Shutdown
		return
	}

	// convert to []MetricElastic
	esMetrics := legacy.ElasticFromBatch(&batch)

	// forward all created MetricElastic to elasticsearch
	for _, esm := range esMetrics {
		var outMsg []byte
		if outMsg, err = json.Marshal(&esm); err != nil {
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

		// check HTTP response error
		if err != nil {
			// signal main to shut down
			d.Death <- err
			<-d.Shutdown
			return
		}

		// check HTTP response statuscode
		if resp.StatusCode() > 299 {
			// signal main to shut down
			d.Death <- fmt.Errorf("ES HTTP response was: %s",
				resp.Status())
			<-d.Shutdown
			return
		}

		// mark successful outgoing message
		out.Mark(1)
	}

	// commit msg offset as processed
	d.delay.Go(func() {
		d.commit(msg)
	})
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
