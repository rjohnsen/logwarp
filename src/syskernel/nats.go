package syskernel

import (
	"encoding/json"
	"log"

	"github.com/nats-io/nats.go"
)

type NatsJobMessage struct {
	Id     string `json:"id"`
	Index  string `json:"index"`
	Log    string `json:"log"`
	Grok   string `json:"grokpattern"`
	Parser string `json:"parser"`
}

type NatsJobError struct {
	Id    string `json:"id"`
	Error string `json:"error"`
}

type NatsJobStatus struct {
	Id      string `json:"id"`
	Status  int    `json:"status"`
	Records int    `json:"records"`
}

func SendError(natsclient *nats.Conn, message NatsJobError) {
	msg, err := json.Marshal(message)

	if err != nil {
		log.Print("ERROR: Unable to Marshal")
	}

	err = natsclient.Publish("logwarp/output", []byte(msg))

	if err != nil {
		log.Printf("ERROR: Failed to send message: %v", err)
	}

	log.Printf("ERROR: %s", message.Error)
}

func SendStatus(natsclient *nats.Conn, status NatsJobStatus) {
	msg, err := json.Marshal(status)

	if err != nil {
		log.Print("ERROR: Unable to Marshal")
	}

	err = natsclient.Publish("logwarp/output", []byte(msg))

	if err != nil {
		log.Printf("ERROR: Failed to send message: %v", err)
	}

	log.Printf("Job: %s status: %d, count: %d)", status.Id, status.Status, status.Records)
}
