package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type stats struct {
	Processed int         `json:"processed"`
	Failed    int         `json:"failed"`
	Jobs      interface{} `json:"jobs"`
}

func Stats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	jobs := make(map[string][]*map[string]interface{})

	for _, m := range managers {
		queue := m.queueName()
		jobs[queue] = make([]*map[string]interface{}, 0)

		for _, worker := range m.workers {
			message := worker.currentMsg
			startedAt := worker.startedAt

			if message != nil && startedAt > 0 {
				jobs[queue] = append(jobs[queue], &map[string]interface{}{
					"message":    message,
					"started_at": startedAt,
				})
			}
		}
	}

	stats := stats{
		0,
		0,
		jobs,
	}

	conn := Config.Pool

	var results []interface{}
	results = append(results, conn.Get(Config.Namespace+"stat:processed").String())
	results = append(results, conn.Get(Config.Namespace+"stat:failed").String())



	if len(results) == 2 {
		if results[0] != nil {
			stats.Processed, _ = strconv.Atoi(string(results[0].([]byte)))
		}
		if results[1] != nil {
			stats.Failed, _ = strconv.Atoi(string(results[1].([]byte)))
		}
	}

	body, _ := json.MarshalIndent(stats, "", "  ")
	fmt.Fprintln(w, string(body))
}
