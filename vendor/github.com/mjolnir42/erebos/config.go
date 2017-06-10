/*-
 * Copyright © 2016-2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package erebos

import (
	"bytes"
	"encoding/json"
	"io/ioutil"

	"github.com/client9/reopen"
	ucl "github.com/nahanni/go-ucl"
)

// Config holds the runtime configuration which is expected to be
// read from a UCL formatted file
type Config struct {
	// Log is the namespace for logging options
	Log struct {
		// Name of the logfile
		File string `json:"file"`
		// Path in wich to open the logfile
		Path string `json:"path"`
		// Reopen the logfile if SIGUSR2 is received
		Rotate bool `json:"rotate.on.usr2,string"`
		// Handle to the logfile
		FH *reopen.FileWriter `json:"-"`
	} `json:"log"`
	// Zookeeper is the namespace with options for Apache Zookeeper
	Zookeeper struct {
		// How often to publish offset updates to Zookeeper
		CommitInterval int `json:"commit.ms,string"`
		// Conncect string for the Zookeeper-Ensemble to use
		Connect string `json:"connect.string"`
		// If true, the Zookeeper stored offset will be ignored and
		// the newest message consumed
		ResetOffset bool `json:"reset.offset.on.startup,string"`
	} `json:"zookeeper"`
	// Kafka is the namespace with options for Apache Kafka
	Kafka struct {
		// Name of the consumergroup to join
		ConsumerGroup string `json:"consumer.group.name"`
		// Which topics to consume from
		ConsumerTopics string `json:"consumer.topics"`
		// Where to start consuming: Oldest, Newest
		ConsumerOffsetStrategy string `json:"consumer.offset.strategy"`
		// Which topic to produce to
		ProducerTopic string `json:"producer.topic"`
		// Producer-Response behaviour: NoResponse, WaitForLocal or WaitForAll
		ProducerResponseStrategy string `json:"producer.response.strategy"`
		// Producer retry attempts
		ProducerRetry int `json:"producer.retry.attempts,string"`
		// Keepalive interval in milliseconds
		Keepalive int `json:"keepalive.ms,string"`
	} `json:"kafka"`
	// Twister is the namespace with configuration options relating to
	// the splitting of metric batches
	Twister struct {
		HandlerQueueLength int `json:"handler.queue.length,string"`
	} `json:"twister"`
	// Mistral is the namespace with configuration options relating to
	// accepting incoming messages via HTTP API
	Mistral struct {
		HandlerQueueLength int    `json:"handler.queue.length,string"`
		ListenAddress      string `json:"listen.address"`
		ListenPort         string `json:"listen.port"`
		EndpointPath       string `json:"api.endpoint.path"`
	} `json:"mistral"`
	// DustDevil is the namespace with configuration options relating to
	// forwarding Kafka read messages to an HTTP API
	DustDevil struct {
		HandlerQueueLength int    `json:"handler.queue.length,string"`
		Endpoint           string `json:"api.endpoint"`
		RetryCount         int    `json:"post.request.retry.count,string"`
		RetryMinWaitTime   int    `json:"retry.min.wait.time.ms,string"`
		RetryMaxWaitTime   int    `json:"retry.max.wait.time.ms,string"`
		RequestTimeout     int    `json:"request.timeout.ms,string"`
		StripStringMetrics bool   `json:"strip.string.metrics,string"`
	} `json:"dustdevil"`
}

// FromFile sets Config c based on the file contents
func (c *Config) FromFile(fname string) error {
	var (
		file, uclJSON []byte
		err           error
		fileBytes     *bytes.Buffer
		parser        *ucl.Parser
		uclData       map[string]interface{}
	)
	if file, err = ioutil.ReadFile(fname); err != nil {
		return err
	}

	fileBytes = bytes.NewBuffer(file)
	parser = ucl.NewParser(fileBytes)
	if uclData, err = parser.Ucl(); err != nil {
		return err
	}

	if uclJSON, err = json.Marshal(uclData); err != nil {
		return err
	}
	return json.Unmarshal(uclJSON, &c)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
