/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package dustdevil // import "github.com/mjolnir42/dustdevil/internal/dustdevil"

import (
	"sync"
	"time"

	resty "gopkg.in/resty.v0"

	"github.com/mjolnir42/delay"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/eyewall"
)

// Implementation of the erebos.Handler interface

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

	d.lookup = eyewall.NewLookup(d.Config)
	defer d.lookup.Close()

	d.delay = delay.New()
	d.assemblyLock = sync.Mutex{}
	d.run()
}

// InputChannel returns the data input channel
func (d *DustDevil) InputChannel() chan *erebos.Transport {
	return d.Input
}

// ShutdownChannel returns the shutdown signal channel
func (d *DustDevil) ShutdownChannel() chan struct{} {
	return d.Shutdown
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
