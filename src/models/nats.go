package models

type NatsJobMessage struct {
	Id     string `json:"id"`
	Index  string `json:"index"`
	Log    string `json:"log"`
	Grok   string `json:"grokpattern"`
	Parser string `json:"parser"`
}
