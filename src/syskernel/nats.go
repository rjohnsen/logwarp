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
