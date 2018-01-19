/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package erebos

import "time"

// Transport is a small wrapper struct for byte values with a return
// error channel and metadata
type Transport struct {
	HostID    int
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
	Commit    chan *Commit
	Return    chan error
}

// Commit is the commit notification a processor is expected to send to
// DelayedCommit
type Commit struct {
	Topic     string
	Partition int32
	Offset    int64
}

// NewHeartbeat returns a new *Transport that represents an internal
// heartbeat that can be delivered over the regular datachannel. This
// way the heartbeat can be lost if the normal datapath encounters
// errors.
func NewHeartbeat() *Transport {
	binTime, _ := time.Now().UTC().MarshalBinary()
	return &Transport{
		HostID:    -1,
		Value:     binTime,
		Topic:     `erebos.heartbeat`,
		Partition: -1,
		Offset:    -1,
	}
}

// IsHeartbeat returns true if t is a heartbeat transport
func IsHeartbeat(t *Transport) bool {
	return t.HostID == -1 && t.Topic == `erebos.heartbeat` &&
		t.Partition == -1 && t.Offset == -1
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
