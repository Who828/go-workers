package workers

import (
	"strconv"
	"time"

	"gopkg.in/redis.v3"
)

type config struct {
	processId    string
	Namespace    string
	PollInterval int
	Pool         *redis.ClusterClient
	Fetch        func(queue string) Fetcher
}

var Config *config

func Configure(options map[string]string) {
	var poolSize int
	var namespace string
	var pollInterval int

	if options["process"] == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}
	if options["pool"] == "" {
		options["pool"] = "1"
	}
	if options["namespace"] != "" {
		namespace = options["namespace"] + ":"
	}
	if seconds, err := strconv.Atoi(options["poll_interval"]); err == nil {
		pollInterval = seconds
	} else {
		pollInterval = 15
	}

	poolSize, _ = strconv.Atoi(options["pool"])

	Config = &config{
		options["process"],
		namespace,
		pollInterval,
		redis.NewClusterClient(&redis.ClusterOptions{
			Addrs: []string{options["servers"]},
			PoolSize: poolSize,
			IdleTimeout: 240 * time.Second,
			Password: options["password"],
		}),
		func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}
