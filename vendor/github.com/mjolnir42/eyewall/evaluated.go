/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package eyewall

import (
	"time"
)

// Evaluated updates the timestamp for the evaluation of ID inside
// the local Redis cache
func (l *Lookup) Evaluated(ID string) {
	if _, err := l.redis.HSet(
		`evaluation`,
		ID,
		time.Now().UTC().Format(time.RFC3339),
	).Result(); err != nil {
		if l.log != nil {
			l.log.Errorf("eyewall/evaluated: %s", err.Error())
		}
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
