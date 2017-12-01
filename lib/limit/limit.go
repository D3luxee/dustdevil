/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package limit implements a concurrency limit.
package limit // import "github.com/mjolnir42/dustdevil/lib/limit"

import (
	"sync"
	"sync/atomic"
)

// Limit can be used to limit concurrency on a resource to a specific
// number of goroutines, for example the number of active in-flight
// HTTP requests.
//
//	l := limit.NewLimit(4)
//	...
//	go func() {
//	    l.Start()
//	    defer l.Done()
//	    ... use resource ...
//	}()
//
// Not calling Done() will over time starve l and render the limit
// permanently reached, blocking all Start() requests.
type Limit struct {
	concurrency uint32
	usage       uint32
	lock        *sync.RWMutex
	cond        *sync.Cond
}

// NewLimit returns a new concurrency limit
func NewLimit(parallel uint32) *Limit {
	l := &Limit{
		concurrency: parallel,
		lock:        &sync.RWMutex{},
	}
	l.cond = sync.NewCond(l.lock.RLocker())
	return l
}

// Start signals that the caller wants to utilize the a resource guarded
// by l. It blocks until the caller is free to use the resource.
// The caller must call Done() once finished.
func (l *Limit) Start() {
reattempt:
	// check if they pool is available
	l.cond.L.Lock()
	for !l.available() {
		l.cond.Wait()
	}
	// drop RLock to attempt getting the WLock
	l.cond.L.Unlock()

	// acquire WLock
	l.lock.Lock()
	// recheck the condition
	if !l.available() {
		// drop WLock and attempt again
		l.lock.Unlock()
		goto reattempt
	}
	// increase usage and drop WLock
	atomic.AddUint32(&l.usage, 1)
	l.lock.Unlock()
}

// Done signals that the caller is finished using the resource guarded
// by Limit. It decrements the usage and wakes up all goroutines
// waiting on its availability.
func (l *Limit) Done() {
	broadcast := false

	l.lock.Lock()
	atomic.AddUint32(&l.usage, ^uint32(0))
	// check if the limit is available while holding the WLock, but only
	// wakeup calling goroutines after dropping it
	if l.available() {
		broadcast = true
	}
	l.lock.Unlock()

	if broadcast {
		l.cond.Broadcast()
	}
}

// available checks if Limit l is below its concurrency limit. l.lock
// must be held when calling it, available does not lock on its own. If
// concurrency is set to 0, no limit is applied
func (l *Limit) available() bool {
	// unlimited if concurrency is set to 0
	if l.concurrency == 0 {
		return true
	}
	// check if the usage is below the concurrency limit
	if l.usage < l.concurrency {
		return true
	}
	return false
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
