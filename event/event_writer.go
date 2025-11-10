package event

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

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

/*
	TODO:
		write Sql Writer
*/

type SqlWriter struct {
	dbName string
	db     *sql.DB
}

func (w *SqlWriter) InitWriter(dbName string) error {
	w.dbName = dbName
	psgInfo := fmt.Sprintf("dbname=%s sslmode=disable", w.dbName)
	db, err := sql.Open("postgres", psgInfo)
	if err != nil {
		log.Printf("Error while opening database %v\n", err)
		return err
	}
	w.db = db
	return nil
}

func (w SqlWriter) CloseWrier() {
	w.db.Close()
}

func (writer SqlWriter) WriteEvent(evt *AuditEvent) error {

	stmt, err := writer.db.Prepare("INSERT INTO evt_table (data) VALUES ($1) RETURNING id")
	if err != nil {
		log.Printf("Error while preparing statement %v\n", err)
		return err
	}
	defer stmt.Close()
	payload, err := EncodeEvent(evt)
	if err != nil {
		log.Printf("Can't encode evt %v\n", err)
		return err
	}

	res, err := stmt.Exec(string(payload))
	if err != nil {
		log.Printf("error executing statement %v\n", err)
		return err
	}
	log.Printf("Result %v\n", res)
	log.Printf("Write event %v\n", *evt)
	return nil
}

//TODO: write function below, add ev_session_id, ev_req_id and attrs processing
/* create select request */
func CreateSelectReq(query *AuditEventQuery) (string, bool) {
	statement_cnt := 0
	query_req := "SELECT * FROM evt_table WHERE"
	default_req := "SELECT id, data FROM evt_table ORDER BY RANDOM() LIMIT 1"
	if !query.Ts.IsZero() {
		ts := query.Ts.Format("2006-01-02T15:04:05Z")
		s := fmt.Sprintf(" data->'timestamp' ? '%s'", ts)
		query_req += s
		statement_cnt++
	} else {
		if !query.TsStart.IsZero() {
			ts := query.TsStart.Format("2006-01-02T15:04:05Z")
			s := fmt.Sprintf(" data->>'timestamp' >= '%s'", ts)
			query_req += s
			statement_cnt++
		}
		if !query.TsEnd.IsZero() {
			if statement_cnt != 0 {
				query_req += " AND"
			}
			ts := query.TsEnd.Format("2006-01-02T15:04:05Z")
			s := fmt.Sprintf(" data->>'timestamp' <= '%s'", ts)
			query_req += s
			statement_cnt++
		}
	}

	if len(query.Resource) > 0 {
		if statement_cnt != 0 {
			query_req += " AND"
		}
		query_req += " ("
		for i, resource := range query.Resource {
			if i != 0 {
				query_req += " OR"
			}
			query_req += fmt.Sprintf(" data->'component' ? '%s'", resource)
		}
		query_req += ")"
	}

	if len(query.User) > 0 {
		if statement_cnt != 0 {
			query_req += " AND"
		}
		query_req += " ("
		for i, user := range query.User {
			if i != 0 {
				query_req += " OR"
			}
			query_req += fmt.Sprintf(" data->'user' ? '%s'", user)
		}
		query_req += ")"
	}

	if len(query.Operation) > 0 {
		if statement_cnt != 0 {
			query_req += " AND"
		}
		query_req += " ("
		for i, op := range query.Operation {
			if i != 0 {
				query_req += " OR"
			}
			query_req += fmt.Sprintf(" data->'op' ? '%s'", op)
		}
		query_req += ")"
	}
	if statement_cnt == 0 {
		return default_req, true
	} else {
		return query_req, true
	}
}

/* TODO: write function correctly */
// For now just read random entry from table
func (writer SqlWriter) ReadEvent(query *AuditEventQuery) ([]AuditEvent, error) {
	log.Printf("Read event with query %v\n", *query)
	//var evt AuditEvent
	item := new(Item)
	req, ok := CreateSelectReq(query)
	if !ok {
		log.Printf("Error while creating select req\n")
		return nil, nil
	}
	log.Printf("Use request %s\n", req)
	err := writer.db.QueryRow(req).Scan(&item.ID, &item.Evt)
	if err != nil {
		log.Printf("Error while querying %v\n", err)
		return nil, err
	}

	return []AuditEvent{item.Evt}, nil
}

func (writer SqlWriter) Write(data []byte) error {
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

func (write SqlWriter) Read(data []byte) ([]byte, error) {
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

type Item struct {
	ID  int
	Evt AuditEvent
}

func (a *AuditEvent) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &a)
}
