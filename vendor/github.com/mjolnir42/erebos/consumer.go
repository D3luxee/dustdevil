/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package erebos

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"github.com/wvanbergen/kafka/consumergroup"
	kazoo "github.com/wvanbergen/kazoo-go"
)

// Consumer is a Kafka Consumergroup consumer that sends received
// messages to a Dispatcher
func Consumer(conf *Config, dispatch Dispatcher,
	shutdown, exit chan struct{}, death chan error) {

	kfkConf := consumergroup.NewConfig()
	switch conf.Kafka.ConsumerOffsetStrategy {
	case `Oldest`, `oldest`:
		kfkConf.Offsets.Initial = sarama.OffsetOldest
	case `Newest`, `newest`:
		kfkConf.Offsets.Initial = sarama.OffsetNewest
	default:
		kfkConf.Offsets.Initial = sarama.OffsetNewest
	}
	kfkConf.Offsets.ProcessingTimeout = 10 * time.Second
	kfkConf.Offsets.CommitInterval = time.Duration(
		conf.Zookeeper.CommitInterval,
	) * time.Millisecond
	kfkConf.Offsets.ResetOffsets = conf.Zookeeper.ResetOffset

	var zkNodes []string
	zkNodes, kfkConf.Zookeeper.Chroot = kazoo.ParseConnectionString(
		conf.Zookeeper.Connect,
	)

	consumerTopic := strings.Split(conf.Kafka.ConsumerTopics, `,`)
	consumer, err := consumergroup.JoinConsumerGroup(
		conf.Kafka.ConsumerGroup,
		consumerTopic,
		zkNodes,
		kfkConf,
	)
	if err != nil {
		death <- err
		return
	}

	offsets := make(map[string]map[int32]int64)

runloop:
	for {
		select {
		case err := <-consumer.Errors():
			death <- err
			break runloop
		case <-shutdown:
			break runloop
		case msg := <-consumer.Messages():
			if offsets[msg.Topic] == nil {
				offsets[msg.Topic] = make(map[int32]int64)
			}

			if offsets[msg.Topic][msg.Partition] != 0 &&
				offsets[msg.Topic][msg.Partition] != msg.Offset-1 {
				// incorrect offset
				logrus.Warnf("Unexpected offset on %s:%d. "+
					"Expected %d, found %d.\n",
					msg.Topic,
					msg.Partition,
					offsets[msg.Topic][msg.Partition]+1,
					msg.Offset,
				)
			}
			dispatch(Transport{Value: msg.Value})

			offsets[msg.Topic][msg.Partition] = msg.Offset
			consumer.CommitUpto(msg)
		}
	}
	// not sending the error into the death channel since shutdown
	// is already initiated
	if err := consumer.Close(); err != nil {
		logrus.Errorf("Error closing the consumer: %s", err.Error())
	}
	// in the error case, wait for the shutdown
	<-shutdown
	close(exit)
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
