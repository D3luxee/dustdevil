/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package erebos

// Transport is a small wrapper struct for byte values with a return
// error channel and metadata
type Transport struct {
	HostID int
	Value  []byte
	Return chan error
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
