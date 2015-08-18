package workers

import (
	"time"
)

type MiddlewareStats struct{}

func (l *MiddlewareStats) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	defer func() {
		if e := recover(); e != nil {
			incrementStats("failed")
			panic(e)
		}
	}()

	acknowledge = next()

	incrementStats("processed")

	return
}

func incrementStats(metric string) {
	conn := Config.Pool

	today := time.Now().UTC().Format("2006-01-02")

	_, err := conn.Incr(Config.Namespace+"stat:"+metric).Result()
	if err != nil {
		Logger.Println("couldn't save stats:", err)
	}
	_, err = conn.Incr(Config.Namespace+"stat:"+metric+":"+today).Result()

	if err != nil {
		Logger.Println("couldn't save stats:", err)
	}
}
