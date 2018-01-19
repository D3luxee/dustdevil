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
)

// assembleSplit is the handler for assembling MetricSplit messages
func (d *DustDevil) assembleSplit(msg *erebos.Transport) {
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
	split := legacy.MetricSplit{}
	if err = json.Unmarshal(msg.Value, &split); err != nil {
		// signal main to shut down
		d.Death <- err
		<-d.Shutdown
		return
	}

	// check data structures are set up
	if _, ok := d.assembly[msg.HostID]; !ok {
		d.assembly[msg.HostID] = make(map[time.Time]legacy.MetricData)
	}
	if _, ok := d.assembly[msg.HostID][split.TS]; !ok {
		data := legacy.MetricData{}
		data.Time = split.TS
		data.FloatMetrics = make([]legacy.FloatMetric, 0)
		data.StringMetrics = make([]legacy.StringMetric, 0)
		data.IntMetrics = make([]legacy.IntMetric, 0)
		d.assembly[msg.HostID][split.TS] = data
	}
	if _, ok := d.assemblyCommit[msg.HostID]; !ok {
		d.assemblyCommit[msg.HostID] = make([]*erebos.Transport, 0)
	}

	// skip string metric reassembly if they are to be stripped,
	// commit and return
	if d.Config.DustDevil.StripStringMetrics && split.Type == `string` {
		d.delay.Go(func() {
			d.commit(msg)
		})
		return
	}

	if len(split.Tags) == 0 {
		split.Tags = []string{``}
	}

	for _, tag := range split.Tags {
		m := d.assembly[msg.HostID][split.TS]
		switch split.Type {
		case `real`:
			m.FloatMetrics = append(m.FloatMetrics,
				legacy.FloatMetric{
					Metric:  split.Path,
					Subtype: tag,
					Value:   split.Val.FlpVal,
				})
		case `integer`, `long`:
			m.IntMetrics = append(m.IntMetrics,
				legacy.IntMetric{
					Metric:  split.Path,
					Subtype: tag,
					Value:   split.Val.IntVal,
				})
		case `string`:
			m.StringMetrics = append(m.StringMetrics,
				legacy.StringMetric{
					Metric:  split.Path,
					Subtype: tag,
					Value:   split.Val.StrVal,
				})
		}
		d.assembly[msg.HostID][split.TS] = m
	}
	d.assemblyCommit[msg.HostID] = append(
		d.assemblyCommit[msg.HostID],
		msg,
	)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
