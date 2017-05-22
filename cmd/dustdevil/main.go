/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main // import "github.com/mjolnir42/dustdevil/cmd/dustdevil"

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mjolnir42/dustdevil/lib/dustdevil"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/legacy"
	"github.com/wvanbergen/kafka/consumergroup"
	kazoo "github.com/wvanbergen/kazoo-go"
)

func main() {
	ddConf := erebos.Config{}
	if err := ddConf.FromFile(`dustdevil.conf`); err != nil {
		log.Fatalln(err)
	}

	kfkConf := consumergroup.NewConfig()
	kfkConf.Offsets.Initial = sarama.OffsetNewest
	kfkConf.Offsets.ProcessingTimeout = 10 * time.Second
	kfkConf.Offsets.CommitInterval = time.Duration(
		ddConf.Zookeeper.CommitInterval,
	) * time.Millisecond
	kfkConf.Offsets.ResetOffsets = ddConf.Zookeeper.ResetOffset

	var zkNodes []string
	zkNodes, kfkConf.Zookeeper.Chroot = kazoo.ParseConnectionString(
		ddConf.Zookeeper.Connect,
	)

	consumerTopic := strings.Split(ddConf.Kafka.ConsumerTopics, `,`)
	consumer, err := consumergroup.JoinConsumerGroup(
		ddConf.Kafka.ConsumerGroup,
		consumerTopic,
		zkNodes,
		kfkConf,
	)
	if err != nil {
		log.Fatalln(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	// this channel is closed by the handler on error
	handlerDeath := make(chan struct{})

	offsets := make(map[string]map[int32]int64)
	handlers := make(map[int]dustdevil.DustDevil)

	for i := 0; i < runtime.NumCPU(); i++ {
		h := dustdevil.DustDevil{
			Num: i,
			Input: make(chan []byte,
				ddConf.DustDevil.HandlerQueueLength),
			Shutdown: make(chan struct{}),
			Death:    handlerDeath,
			Config:   &ddConf,
		}
		handlers[i] = h
		go h.Start()
	}

	fault := false
	heartbeat := time.Tick(1 * time.Second)
	modulus := runtime.NumCPU()

runloop:
	for {
		select {
		case <-c:
			for i := range handlers {
				close(handlers[i].Shutdown)
			}
			break runloop
		case <-handlerDeath:
			for i := range handlers {
				close(handlers[i].Shutdown)
			}
			break runloop
		case <-heartbeat:
			continue runloop
		case e := <-consumer.Errors():
			log.Println(e)
			fault = true
			break runloop
		case msg := <-consumer.Messages():
			if offsets[msg.Topic] == nil {
				offsets[msg.Topic] = make(map[int32]int64)
			}

			if offsets[msg.Topic][msg.Partition] != 0 &&
				offsets[msg.Topic][msg.Partition] != msg.Offset-1 {
				// incorrect offset
				log.Printf("Unexpected offset on %s:%d. "+
					"Expected %d, found %d.\n",
					msg.Topic,
					msg.Partition,
					offsets[msg.Topic][msg.Partition]+1,
					msg.Offset,
				)
			}

			// send all messages from the same host to the same handler
			// to keep the ordering intact
			hostID, err := legacy.PeekHostID(msg.Value)
			if err != nil {
				log.Println(err)
				fault = true
				break runloop
			}
			handlers[hostID%modulus].Input <- msg.Value

			offsets[msg.Topic][msg.Partition] = msg.Offset
			consumer.CommitUpto(msg)
		}
	}
	if err := consumer.Close(); err != nil {
		log.Println(`Error closing the consumer:`, err)
	}
	if fault {
		// let the service supervisor know the shutdown was not
		// planned
		os.Exit(1)
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
