/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package legacy

import "sync"

// Split breaks up a MetricBatch into a slice of individual
// MetricSplit message
func (m *MetricBatch) Split() []MetricSplit {
	res := []MetricSplit{}
	for iter := range m.Data {
		// size the channel the split up structs will be put into
		length := len(m.Data[iter].FloatMetrics) +
			len(m.Data[iter].StringMetrics) +
			len(m.Data[iter].IntMetrics)
		collect := make(chan MetricSplit, length)

		// collector go routine that will run indefinitly until the
		// channel is closed
		cwg := sync.WaitGroup{}
		go func() {
			cwg.Add(1)
			for elem := range collect {
				res = append(res, elem)
			}
		}()

		// feeder go routines that will convert the slices and push
		// the result into channel collect
		wg := sync.WaitGroup{}
		// convert float metrics
		go func() {
			wg.Add(1)
			split := MetricSplit{
				AssetID: int64(m.HostID),
				TS:      m.Data[iter].Time,
			}
			for _, fMetric := range m.Data[iter].FloatMetrics {
				split.Type = `real`
				split.Path = fMetric.Metric
				split.Val.FlpVal = fMetric.Value
				split.Tags = []string{fMetric.Subtype}
				collect <- split
			}
		}()

		// convert string metrics
		go func() {
			wg.Add(1)
			split := MetricSplit{
				AssetID: int64(m.HostID),
				TS:      m.Data[iter].Time,
			}
			for _, sMetric := range m.Data[iter].StringMetrics {
				split.Type = `string`
				split.Path = sMetric.Metric
				split.Val.StrVal = sMetric.Value
				split.Tags = []string{sMetric.Subtype}
				collect <- split
			}
		}()

		// convert integer metrics
		go func() {
			wg.Add(1)
			split := MetricSplit{
				AssetID: int64(m.HostID),
				TS:      m.Data[iter].Time,
			}
			for _, iMetric := range m.Data[iter].IntMetrics {
				split.Type = `integer`
				split.Path = iMetric.Metric
				split.Val.IntVal = iMetric.Value
				split.Tags = []string{iMetric.Subtype}
				collect <- split
			}
		}()
		// wait for feeder
		wg.Wait()
		// close collect channel to unblock the collector
		close(collect)
		// wait for collector
		cwg.Wait()
	}
	return res
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
