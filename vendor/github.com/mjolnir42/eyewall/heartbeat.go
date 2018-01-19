/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package eyewall

import (
	"fmt"
	"sync"
	"time"
)

// heartbeatMap is a locked map that keeps track of timestamps
type heartbeatMap struct {
	hb   map[int]time.Time
	lock sync.Mutex
}

// Heartbeat updates an application heartbeat message inside the
// Redis cache. handlerNum -1 is reserved.
func (l *Lookup) Heartbeat(appname string, handlerNum int, binTime []byte) {
	ts := time.Time{}
	if err := ts.UnmarshalBinary(binTime); err != nil {
		if l.log != nil {
			l.log.Errorf("eyewall/heartbeat: %s", err.Error())
		}
	}

	// lock shared map
	beats.lock.Lock()
	defer beats.lock.Unlock()

	if beats.hb[-1].IsZero() || ts.After(beats.hb[-1]) {
		beats.hb[-1] = ts
		l.updateRedisHB(-1, fmt.Sprintf(
			"%s-alive", appname),
		)
	}
	if ts.After(beats.hb[handlerNum]) {
		beats.hb[handlerNum] = ts
		l.updateRedisHB(handlerNum, fmt.Sprintf(
			"%s-alive-%d", appname, handlerNum),
		)
	}
}

// updateRedisHB performs the heartbeat update in Redis
func (l *Lookup) updateRedisHB(num int, key string) {
	if _, err := l.redis.HSet(
		`heartbeat`,
		key,
		beats.hb[num].UTC().Format(time.RFC3339),
	).Result(); err != nil {
		if l.log != nil {
			l.log.Errorf("eyewall/heartbeat: %s", err.Error())
		}
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
