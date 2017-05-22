/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package legacy implements decoding routines for legacy metric
// formats.
package legacy

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/davecgh/go-spew/spew"
)

// Debug set to true means unexpected input will be skipped and
// spewed to StdErr. When set to false, it will panic instead.
var Debug = false

// MetricBatch represents a single request payload as sent by the
// client application. It can contain multiple measurement cycles,
// each in turn consisting of many individual metrics.
type MetricBatch struct {
	HostID   int          `json:"hostID"`
	Protocol int          `json:"protocol"`
	Data     []MetricData `json:"data"`
}

// MetricData contains the metric data from a single measurement
// time
type MetricData struct {
	Time          time.Time      `json:"time"`
	FloatMetrics  []FloatMetric  `json:"floatMetrics"`
	StringMetrics []StringMetric `json:"stringMetrics"`
	IntMetrics    []IntMetric    `json:"intMetrics"`
}

// FloatMetric represents a single metric value of type float64
type FloatMetric struct {
	Metric  string  `json:"metric"`
	Subtype string  `json:"subtype"`
	Value   float64 `json:"value"`
}

// StringMetric represents a single metric value of type string
type StringMetric struct {
	Metric  string `json:"metric"`
	Subtype string `json:"subtype"`
	Value   string `json:"value"`
}

// IntMetric represents a single metric value of type int64
type IntMetric struct {
	Metric  string `json:"metric"`
	Subtype string `json:"subtype"`
	Value   int64  `json:"value"`
}

// UnmarshalJSON implements the logic required to parse the JSON
// wireformat into a struct.
func (m *MetricBatch) UnmarshalJSON(data []byte) error {
	var hlp metricBatchParse
	if err := json.Unmarshal(data, &hlp); err != nil {
		return err
	}
	m.HostID = hlp.HostID
	m.Protocol = hlp.Protocol
	m.Data = make([]MetricData, 0)

	for i := range hlp.Data {
		data := MetricData{}
		data.FloatMetrics = make([]FloatMetric, 0)
		data.StringMetrics = make([]StringMetric, 0)
		data.Time = time.Unix(hlp.Data[i].CTime, 0).UTC()

		metrics := hlp.Data[i].Metrics.(map[string]interface{})

		for metric, val := range metrics {
			parseMap(val.(map[string]interface{}), &data, metric)
		}

		m.Data = append(m.Data, data)
	}
	return nil
}

// metricBatchParse is the private struct used by
// MetricBatch.UnmarshalJSON to actually parse the data wireformat
type metricBatchParse struct {
	HostID   int               `json:"host_id"`
	Protocol int               `json:"proto_ver"`
	Data     []metricDataParse `json:"data"`
}

// metricDataParse is another parse helper struct to decode the
// MetricBatch wire format
type metricDataParse struct {
	CTime   int64       `json:"ctime"`
	Metrics interface{} `json:"metrics"`
}

// parseMap is used in MetricBatch.UnmarshalJSON to parse
// encountered map[string]interface{} via reflection
func parseMap(m map[string]interface{}, data *MetricData, metric string) {
loop:
	for key, val := range m {
		vval := reflect.ValueOf(&val).Elem()
		switch vval.Elem().Kind().String() {
		case `map`:
			parseMap(val.(map[string]interface{}), data, metric)
		case `slice`:
			parseSlice(val.([]interface{}), data, metric)
		case `string`:
			parseMapString(key, val.(string), metric, data)
		case `float64`:
			parseMapFloat(key, metric, val.(float64), data)
		case `int64`:
			parseMapInt(key, metric, val.(int64), data)
		default:
			msg := fmt.Sprintf("parseMap unknown type: %s",
				vval.Elem().Kind().String())
			if Debug {
				fmt.Fprintln(os.Stderr, msg)
				spew.Fdump(os.Stderr, val)
				continue loop
			}
			panic(msg)
		}
	}
}

// parseSlice is used in MetricBatch.UnmarshalJSON to parse
// encountered []interface{} via reflection
func parseSlice(s []interface{}, data *MetricData, metric string) {
loop:
	for _, val := range s {
		vval := reflect.ValueOf(&val).Elem()
		switch vval.Elem().Kind().String() {
		case `map`:
			parseMap(val.(map[string]interface{}), data, metric)
		case `slice`:
			parseSlice(val.([]interface{}), data, metric)
		default:
			msg := fmt.Sprintf("parseSlice unknown type: %s",
				vval.Elem().Kind().String())
			if Debug {
				fmt.Fprintln(os.Stderr, msg)
				spew.Fdump(os.Stderr, val)
				continue loop
			}
			panic(msg)
		}
	}
}

// parseMapString decodes a single key/value pair key, val with a
// string value
func parseMapString(key, val, metric string, data *MetricData) {
	s := StringMetric{
		Metric:  metric,
		Subtype: key,
		Value:   val,
	}
	data.StringMetrics = append(data.StringMetrics, s)
}

// parseMapFloat decodes a single key/value pair key, val with a
// float64 value. It calls parseMapInt if the float64 value can be
// represented by an int64
func parseMapFloat(key, metric string, val float64, data *MetricData) {
	if floatIsInt(val) {
		parseMapInt(key, metric, int64(val), data)
		return
	}
	f := FloatMetric{
		Metric:  metric,
		Subtype: key,
		Value:   val,
	}
	data.FloatMetrics = append(data.FloatMetrics, f)
}

// parseMapInt decodes a single key/value pair key, val with a
// int64 value.
func parseMapInt(key, metric string, val int64, data *MetricData) {
	i := IntMetric{
		Metric:  metric,
		Subtype: key,
		Value:   val,
	}
	data.IntMetrics = append(data.IntMetrics, i)
}

// floatIsInt returns true if the float64 value can be represented
// by an int64. False otherwise.
func floatIsInt(f float64) bool {
	if f == float64(int64(f)) {
		return true
	}
	return false
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
