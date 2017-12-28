/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package dustdevil // import "github.com/mjolnir42/dustdevil/internal/dustdevil"

import (
	metrics "github.com/rcrowley/go-metrics"
)

// run is the event loop for DustDevil
func (d *DustDevil) run() {
	in := metrics.GetOrRegisterMeter(`/input/messages.per.second`, *d.Metrics)

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
			in.Mark(1)
			d.delay.Go(func() {
				if d.Config.DustDevil.ForwardElastic {
					d.processElastic(msg)
					return
				}
				d.process(msg)
			})
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
			in.Mark(1)
			d.process(msg)
		}
	}
	d.delay.Wait()
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
