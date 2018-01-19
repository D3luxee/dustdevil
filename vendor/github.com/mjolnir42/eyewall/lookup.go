/*-
 * Copyright © 2016,2017, Jörg Pernfuß <code.jpe@gmail.com>
 * Copyright © 2016, 1&1 Internet SE
 * All rights reserved.
 *
 * Use of this source code is governed by a 2-clause BSD license
 * that can be found in the LICENSE file.
 */

// Package eyewall provides a lookup library for threshold
// configurations managed by eye
package eyewall

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/go-redis/redis"
	"github.com/mjolnir42/erebos"
	"github.com/mjolnir42/eyeproto"
)

var (
	// ErrNotFound is returned when the cache contains no matching data
	ErrNotFound = errors.New("eyewall.Lookup: not found")
	// ErrUnconfigured is returned when the cache contains a negative
	// caching entry or Eye returns the absence of a profile to look up
	ErrUnconfigured = errors.New("eyewall.Lookup: unconfigured")
	// beats is the map of heartbeats shared between all instances of
	// Lookup. This way it can be ensured that all instances only move
	// the timestamps forward in time.
	beats heartbeatMap
)

func init() {
	beats.hb = make(map[int]time.Time)
}

// Lookup provides a query library to retrieve data from Eye
type Lookup struct {
	Config       *erebos.Config
	log          *logrus.Logger
	redis        *redis.Client
	cacheTimeout time.Duration
}

// NewLookup returns a new *Lookup
func NewLookup(conf *erebos.Config) *Lookup {
	return &Lookup{
		Config: conf,
		log:    nil,
	}
}

// Start sets up Lookup and connects to Redis
func (l *Lookup) Start() error {
	l.cacheTimeout = time.Duration(
		l.Config.Redis.CacheTimeout,
	) * time.Second
	l.redis = redis.NewClient(&redis.Options{
		Addr:     l.Config.Redis.Connect,
		Password: l.Config.Redis.Password,
		DB:       l.Config.Redis.DB,
	})
	if _, err := l.redis.Ping().Result(); err != nil {
		return err
	}
	return nil
}

// Close shuts down the Redis connection
func (l *Lookup) Close() {
	l.redis.Close()
}

// SetLogger hands Lookup the logger to use
func (l *Lookup) SetLogger(logger *logrus.Logger) {
	l.log = logger
}

// GetConfigurationID returns matching monitoring profile ConfigurationIDs
// if any exist.
func (l *Lookup) GetConfigurationID(lookID string) ([]string, error) {
	IDList := []string{}

	// try to serve the request from the local redis cache
	thresh, err := l.processRequest(lookID)
	if err == ErrUnconfigured {
		return IDList, err
	}

	for k := range thresh {
		IDList = append(IDList, thresh[k].ID)
	}
	return IDList, nil
}

// LookupThreshold queries the full monitoring profile data
// for lookID
func (l *Lookup) LookupThreshold(lookID string) (map[string]Threshold, error) {
	return l.processRequest(lookID)
}

// WaitEye returns a channel that it closes once Eye returns a
// valid result without errors
func (l *Lookup) WaitEye() chan struct{} {
	ret := make(chan struct{})
	go func(ret chan struct{}) {
		retryDelay := 50 * time.Millisecond
		client := &http.Client{}
		for {
			<-time.After(retryDelay)
			retryDelay = 5 * time.Second

			req, err := http.NewRequest(`GET`, fmt.Sprintf(
				"http://%s:%s/api/v1/item/",
				l.Config.Eyewall.Host,
				l.Config.Eyewall.Port,
			), nil)
			if err != nil {
				continue
			}
			var resp *http.Response
			if resp, err = client.Do(req); err != nil {
				ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				continue
			}
			// allow connection reuse
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				break
			}
		}
		close(ret)
	}(ret)
	return ret
}

// processRequest handles the multi-stage lookup of querying the
// cache, the profile server and keeps the cache updated
func (l *Lookup) processRequest(lookID string) (map[string]Threshold, error) {
	// fetch from local cache
	thr, err := l.lookupRedis(lookID)
	if err == nil {
		return thr, nil
	} else if err == ErrUnconfigured {
		return nil, ErrUnconfigured
	} else if err != ErrNotFound {
		// genuine error condition, defer to profile server
		if l.log != nil {
			l.log.Errorf("eyewall/cache: %s", err.Error())
		}
	}

	// local cache did not hit or was not available
	// fetch from eye
	cnf, err := l.lookupEye(lookID)
	if err == ErrUnconfigured {
		return nil, ErrUnconfigured
	} else if err != nil {
		return nil, err
	}

	// process result from eye and store in redis
	thr, err = l.process(lookID, cnf)
	if err == ErrUnconfigured {
		return nil, ErrUnconfigured
	} else if err != nil {
		return nil, err
	}
	return thr, nil
}

// lookupRedis queries the Redis profile cache
func (l *Lookup) lookupRedis(lookID string) (map[string]Threshold, error) {
	res := make(map[string]Threshold)
	data, err := l.redis.HGetAll(lookID).Result()
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, ErrNotFound
	}
dataloop:
	for key := range data {
		if key == `unconfigured` {
			if len(data) == 1 {
				return nil, ErrUnconfigured
			}
			continue dataloop
		}
		val, err := l.redis.Get(key).Result()
		if err != nil {
			return nil, err
		}

		t := Threshold{}
		err = json.Unmarshal([]byte(val), &t)
		if err != nil {
			return nil, err
		}
		res[t.ID] = t
	}
	return res, nil
}

// lookupEye queries the Eye monitoring profile server
func (l *Lookup) lookupEye(lookID string) (*eyeproto.ConfigurationData, error) {
	client := &http.Client{}
	req, err := http.NewRequest(`GET`, fmt.Sprintf(
		"http://%s:%s/%s/%s",
		l.Config.Eyewall.Host,
		l.Config.Eyewall.Port,
		l.Config.Eyewall.Path,
		lookID,
	), nil)
	if err != nil {
		return nil, err
	}

	var resp *http.Response
	defer resp.Body.Close()
	if resp, err = client.Do(req); err != nil {
		return nil, err
	} else if resp.StatusCode == 400 {
		return nil, fmt.Errorf(`Lookup: malformed LookupID`)
	} else if resp.StatusCode == 404 {
		l.setUnconfigured(lookID)
		return nil, ErrUnconfigured
	} else if resp.StatusCode >= 500 {
		return nil, fmt.Errorf(
			"Lookup: server error from eye: %d",
			resp.StatusCode,
		)
	}
	var buf []byte
	buf, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	data := &eyeproto.ConfigurationData{}
	err = json.Unmarshal(buf, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// process converts t into Threshold and stores it in the
// local cache if available
func (l *Lookup) process(lookID string, t *eyeproto.ConfigurationData) (map[string]Threshold, error) {
	if t.Configurations == nil {
		return nil, fmt.Errorf(`lookup.process received t.Configurations == nil`)
	}
	if len(t.Configurations) == 0 {
		l.setUnconfigured(lookID)
		return nil, ErrUnconfigured
	}
	res := make(map[string]Threshold)
	for _, i := range t.Configurations {
		t := Threshold{
			ID:             i.ConfigurationItemID,
			Metric:         i.Metric,
			HostID:         i.HostID,
			Oncall:         i.Oncall,
			Interval:       i.Interval,
			MetaMonitoring: i.Metadata.Monitoring,
			MetaTeam:       i.Metadata.Team,
			MetaSource:     i.Metadata.Source,
			MetaTargethost: i.Metadata.Targethost,
		}
		t.Thresholds = make(map[string]int64)
		for _, tl := range i.Thresholds {
			lvl := strconv.FormatUint(uint64(tl.Level), 10)
			t.Predicate = tl.Predicate
			t.Thresholds[lvl] = tl.Value
		}
		l.storeThreshold(lookID, &t)
		res[t.ID] = t
	}
	return res, nil
}

// setUnconfigured writes a negative cache entry into the local cache
func (l *Lookup) setUnconfigured(lookID string) {
	if _, err := l.redis.HSet(
		lookID,
		`unconfigured`,
		time.Now().UTC().Format(time.RFC3339),
	).Result(); err != nil {
		if l.log != nil {
			l.log.Errorf("eyewall/cache: %s", err.Error())
		}
		return
	}

	if _, err := l.redis.Expire(
		lookID,
		l.cacheTimeout,
	).Result(); err != nil {
		if l.log != nil {
			l.log.Errorf("eyewall/cache: %s", err.Error())
		}
	}
}

// storeThreshold writes t into the local cache
func (l *Lookup) storeThreshold(lookID string, t *Threshold) {
	buf, err := json.Marshal(t)
	if err != nil {
		if l.log != nil {
			l.log.Errorf("eyewall/cache: %s", err.Error())
		}
		return
	}

	if _, err := l.redis.Set(
		t.ID,
		string(buf),
		l.cacheTimeout,
	).Result(); err != nil {
		if l.log != nil {
			l.log.Errorf("eyewall/cache: %s", err.Error())
		}
		return
	}

	if _, err := l.redis.HSet(
		lookID,
		t.ID,
		time.Now().UTC().Format(time.RFC3339),
	).Result(); err != nil {
		if l.log != nil {
			l.log.Errorf("eyewall/cache: %s", err.Error())
		}
		return
	}

	if _, err := l.redis.Expire(
		lookID,
		l.cacheTimeout,
	).Result(); err != nil {
		if l.log != nil {
			l.log.Errorf("eyewall/cache: %s", err.Error())
		}
	}
}

// vim: ts=4 sw=4 sts=4 noet fenc=utf-8 ffs=unix
