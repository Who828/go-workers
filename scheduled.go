package workers

import (
	"strings"
	"time"

	"gopkg.in/redis.v3"
	"strconv"
)

type scheduled struct {
	keys   []string
	closed bool
	exit   chan bool
}

func (s *scheduled) start() {
	go (func() {
		for {
			if s.closed {
				return
			}

			s.poll()

			time.Sleep(time.Duration(Config.PollInterval) * time.Second)
		}
	})()
}

func (s *scheduled) quit() {
	s.closed = true
}

func (s *scheduled) poll() {
	conn := Config.Pool

	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = Config.Namespace + key
		for {
			messages, _ := conn.ZRangeByScore(key, redis.ZRangeByScore{
				Min: "-inf",
				Max: strconv.FormatFloat(now, 'f', 2, 64),
				Offset: 0,
				Count: 1,
			}).Result()

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			if removed, _ := conn.ZRem(key, messages[0]).Result(); removed != -1 {
				queue, _ := message.Get("queue").String()
				queue = strings.TrimPrefix(queue, Config.Namespace)
				message.Set("enqueued_at", nowToSecondsWithNanoPrecision())
				conn.LPush(Config.Namespace+"queue:"+queue, message.ToJson())
			}
		}
	}

	conn.Close()
}

func newScheduled(keys ...string) *scheduled {
	return &scheduled{keys, false, make(chan bool)}
}
