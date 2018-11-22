/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package dustdevil // import "github.com/solnx/dustdevil/internal/dustdevil"

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	metrics "github.com/rcrowley/go-metrics"
)

// postResult is a transport wrapper for assemblePost functions
type postResult struct {
	hostID int
	err    error
}

// release triggers the reassembly of cached metrics and forwarding
// the result
func (d *DustDevil) release() {
	resC := make(chan *postResult)
	wg := sync.WaitGroup{}
	for hostID := range d.assembly {
		wg.Add(1)
		switch d.Config.DustDevil.ForwardElastic {
		case true:
			go func(ID int) {
				d.assemblePostElastic(ID, resC)
				wg.Done()
			}(hostID)
		default:
			go func(ID int) {
				d.assemblePost(ID, resC)
				wg.Done()
			}(hostID)
		}
	}
	// wait for all assemblePost
	wg.Wait()

	// process results
	close(resC)
	shutdown := false

resLoop:
	for res := range resC {
		if res.err != nil {
			if !shutdown {
				// only send first error to main
				d.Death <- res.err
				shutdown = true
			}
			continue resLoop
		}

		// clear message store for hostID
		delete(d.assembly, res.hostID)

		// ACK d.assemblyCommit for hostID
		for i := range d.assemblyCommit[res.hostID] {
			msg := d.assemblyCommit[res.hostID][i]
			d.delay.Go(func() {
				d.commit(msg)
			})
		}

		// clear transport wrapper store for hostID
		d.assemblyCommit[res.hostID] = make([]*erebos.Transport, 0)
	}

	if shutdown {
		<-d.Shutdown
	}
}

// assemblePost constructs legacy.MetricBatch for hostID and
// forwards it via http.Post
func (d *DustDevil) assemblePost(hostID int, resC chan *postResult) {
	var err error
	if len(d.assembly[hostID]) == 0 {
		resC <- &postResult{
			hostID: hostID,
			err:    nil,
		}
		return
	}

	batch := legacy.MetricBatch{
		HostID:   hostID,
		Protocol: 1,
	}
	batch.Data = make([]legacy.MetricData, 0)
	for ts := range d.assembly[hostID] {
		batch.Data = append(batch.Data, d.assembly[hostID][ts])
	}

	var outMsg []byte
	if outMsg, err = batch.MarshalJSON(); err != nil {
		resC <- &postResult{
			hostID: hostID,
			err:    err,
		}
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
		resC <- &postResult{
			hostID: hostID,
			err:    err,
		}
		return
	}
	if resp.StatusCode() > 299 {
		// signal main to shut down
		resC <- &postResult{
			hostID: hostID,
			err: fmt.Errorf("HTTP response was: %s",
				resp.Status()),
		}
		return
	}

	metrics.GetOrRegisterMeter(`/output/messages.per.second`,
		*d.Metrics).Mark(1)

	resC <- &postResult{
		hostID: hostID,
		err:    nil,
	}
}

// assemblePostElastic constructs legacy.MetricElastic for hostID
// and sends it via http.Post to ElasticSearch
func (d *DustDevil) assemblePostElastic(hostID int, resC chan *postResult) {
	var err error
	if len(d.assembly[hostID]) == 0 {
		resC <- &postResult{
			hostID: hostID,
			err:    nil,
		}
		return
	}

	out := metrics.GetOrRegisterMeter(`/output/messages.per.second`,
		*d.Metrics)

	batch := legacy.MetricBatch{
		HostID:   hostID,
		Protocol: 1,
	}
	batch.Data = []legacy.MetricData{}
	for ts := range d.assembly[hostID] {
		batch.Data = append(batch.Data, d.assembly[hostID][ts])
	}

	// convert to []MetricElastic
	esMetrics := legacy.ElasticFromBatch(&batch)

	// forward all created MetricElastic to elasticsearch
	for _, esm := range esMetrics {
		var outMsg []byte
		if outMsg, err = json.Marshal(&esm); err != nil {
			resC <- &postResult{
				hostID: hostID,
				err:    err,
			}
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
		resp, lErr := r.SetBody(outMsg).
			Post(d.Config.DustDevil.Endpoint)

		// release resource limit
		d.Limit.Done()

		// check HTTP response error
		if lErr != nil {
			resC <- &postResult{
				hostID: hostID,
				err:    lErr,
			}
			return
		}

		// check HTTP response statuscode
		if resp.StatusCode() > 299 {
			// signal main to shut down
			resC <- &postResult{
				hostID: hostID,
				err: fmt.Errorf("ES HTTP response was: %s",
					resp.Status()),
			}
			return
		}

		// mark successful outgoing message
		out.Mark(1)
	}
	resC <- &postResult{
		hostID: hostID,
		err:    nil,
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
