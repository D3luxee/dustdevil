/*-
 * Copyright © 2017, Jörg Pernfuß <code.jpe@gmail.com>
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

package main // import "github.com/mjolnir42/dustdevil/cmd/dustdevil"

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/client9/reopen"
	"github.com/mjolnir42/dustdevil/lib/dustdevil"
	"github.com/mjolnir42/erebos"
	metrics "github.com/rcrowley/go-metrics"
)

func init() {
	// Discard logspam from Zookeeper library
	erebos.DisableZKLogger()

	// set standard logger options
	erebos.SetLogrusOptions()
}

func main() {
	// parse command line flags
	var cliConfPath string
	flag.StringVar(&cliConfPath, `config`, `dustdevil.conf`,
		`Configuration file location`)
	flag.Parse()

	// read runtime configuration
	ddConf := erebos.Config{}
	if err := ddConf.FromFile(cliConfPath); err != nil {
		logrus.Fatalf("Could not open configuration: %s", err)
	}

	// setup logfile
	if lfh, err := reopen.NewFileWriter(
		filepath.Join(ddConf.Log.Path, ddConf.Log.File),
	); err != nil {
		logrus.Fatalf("Unable to open logfile: %s", err)
	} else {
		ddConf.Log.FH = lfh
	}
	logrus.SetOutput(ddConf.Log.FH)
	logrus.Infoln(`Starting DUSTDEVIL...`)

	// signal handler will reopen logfile on USR2 if requested
	if ddConf.Log.Rotate {
		sigChanLogRotate := make(chan os.Signal, 1)
		signal.Notify(sigChanLogRotate, syscall.SIGUSR2)
		go erebos.Logrotate(sigChanLogRotate, ddConf)
	}

	// setup signal receiver for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// this channel is used by the handlers on error
	handlerDeath := make(chan error)
	// this channel is used to signal the consumer to stop
	consumerShutdown := make(chan struct{})
	// this channel will be closed by the consumer
	consumerExit := make(chan struct{})

	// setup metrics
	pfxRegistry := metrics.NewPrefixedRegistry(`dustdevil`)
	metrics.NewRegisteredMeter(`.messages`, pfxRegistry)
	metricShutdown := make(chan struct{})

	go func(r *metrics.Registry) {
		beat := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-beat.C:
				(*r).Each(printMetrics)
			case <-metricShutdown:
				break
			}
		}
	}(&pfxRegistry)

	// start application handlers
	for i := 0; i < runtime.NumCPU(); i++ {
		h := dustdevil.DustDevil{
			Num: i,
			Input: make(chan *erebos.Transport,
				ddConf.DustDevil.HandlerQueueLength),
			Shutdown: make(chan struct{}),
			Death:    handlerDeath,
			Config:   &ddConf,
			Metrics:  &pfxRegistry,
		}
		dustdevil.Handlers[i] = &h
		go h.Start()
		logrus.Infof("Launched Dustdevil handler #%d", i)
	}

	// start kafka consumer
	go erebos.Consumer(
		&ddConf,
		dustdevil.Dispatch,
		consumerShutdown,
		consumerExit,
		handlerDeath,
	)

	// the main loop
	fault := false
runloop:
	for {
		select {
		case <-c:
			logrus.Infoln(`Received shutdown signal`)
			break runloop
		case err := <-handlerDeath:
			logrus.Errorf("Handler died: %s", err.Error())
			fault = true
			break runloop
		}
	}

	// close all handlers
	close(metricShutdown)
	close(consumerShutdown)
	<-consumerExit // not safe to close InputChannel before consumer is gone
	for i := range dustdevil.Handlers {
		close(dustdevil.Handlers[i].ShutdownChannel())
		close(dustdevil.Handlers[i].InputChannel())
	}

	// read all additional handler errors if required
drainloop:
	for {
		select {
		case err := <-handlerDeath:
			logrus.Errorf("Handler died: %s", err.Error())
		case <-time.After(time.Millisecond * 10):
			break drainloop
		}
	}

	// give goroutines that were blocked on handlerDeath channel
	// a chance to exit
	<-time.After(time.Millisecond * 10)
	logrus.Infoln(`DUSTDEVIL shutdown complete`)
	if fault {
		os.Exit(1)
	}
}

func printMetrics(metric string, v interface{}) {
	switch v.(type) {
	case *metrics.StandardMeter:
		value := v.(*metrics.StandardMeter)
		fmt.Fprintf(os.Stderr, "%s.avg.rate.1min: %f\n", metric, value.Rate1())
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
