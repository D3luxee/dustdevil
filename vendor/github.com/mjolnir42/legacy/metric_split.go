/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package legacy
import (
	"encoding/json"
	"time"
)

// MetricSplit represents a de-batched, self-contained metric from
// a MetricBatch, suitable for forwarding towards event processing
type MetricSplit struct {
	AssetID int64
	Path    string
	TS      time.Time
	Type    string
	Unit    string
	Val     MetricSplitValue
	Tags    []string
	Labels  map[string]string
}

// MetricSplitValue contains the value of MetricSplit with support
// for multiple value types
type MetricSplitValue struct {
	IntVal int64
	StrVal string
	FlpVal float64
}

// MarshalJSON marshals a struct MetricSplit into the reduced wire
// format expected by downstream consumers
func (m *MetricSplit) MarshalJSON() ([]byte, error) {
	o := make([]interface{}, 8)
	o[0] = m.AssetID
	o[1] = m.Path
	o[2] = m.TS.Format(time.RFC3339)
	o[3] = m.Type
	o[4] = m.Unit
	o[6] = m.Tags
	o[7] = m.Labels
	switch m.Type {
	case `integer`:
		fallthrough
	case `long`:
		o[5] = m.Val.IntVal
	case `real`:
		o[5] = m.Val.FlpVal
	case `string`:
		o[5] = m.Val.StrVal
	}
	return json.Marshal(&o)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
