package event

import (
	"encoding/json"
	"log"

	_ "github.com/lib/pq"
)

type EventWriter interface {
	WriteEvent(*AuditEvent) error                     /* write particular event */
	ReadEvent(*AuditEventQuery) ([]AuditEvent, error) /* read event, with fields*/
}

type TestWriter struct {
}

func (writer TestWriter) WriteEvent(evt *AuditEvent) error {
	log.Printf("Write event %v\n", *evt)
	return nil
}

func (writer TestWriter) ReadEvent(query *AuditEventQuery) ([]AuditEvent, error) {
	log.Printf("Read event with query %v\n", *query)
	var evt AuditEvent
	return []AuditEvent{evt}, nil
}

func (writer TestWriter) Write(data []byte) error {
	evt, err := DecodeEvent(data)
	if err != nil {
		return err
	}
	err = writer.WriteEvent(evt)
	if err != nil {
		return err
	}
	return nil
}

func (write TestWriter) Read(data []byte) ([]byte, error) {
	query, err := DecodeEventQuery(data)
	if err != nil {
		return nil, err
	}
	events, err := write.ReadEvent(query)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(events)
	if err != nil {
		return nil, err
	}
	return payload, nil
}
