package event

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type SqlAuditEvent AuditEvent

type Item struct {
	ID  int
	Evt SqlAuditEvent
}

const (
	AND_OP = " AND "
	OR_OP  = " OR "
)

func (a *SqlAuditEvent) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &a)
}

type PostgresWriter struct {
	dbName string
	db     *sql.DB
}

func (w *PostgresWriter) InitWriter(dbName string) error {
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

func (w PostgresWriter) CloseWrier() {
	w.db.Close()
}

func (writer PostgresWriter) WriteEvent(evt *AuditEvent) error {

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

func CreateStringsConditionString(values []string, field string) string {
	result := "("
	for i, value := range values {
		if i != 0 {
			result += OR_OP
		}
		result += fmt.Sprintf("data->'%s' ? '%s'", field, value)
	}
	result += ")"
	return result
}

func CreateNumericConditionString(values []uint64, field string) string {
	result := "("
	for i, value := range values {
		if i != 0 {
			result += OR_OP
		}

		result += fmt.Sprintf("(data->'%s')::numeric = %d", field, value)
	}
	result += ")"
	return result
}

func CreateSelectReq(query *AuditEventQuery) (string, bool) {
	statement_cnt := 0
	query_req := "SELECT * FROM evt_table WHERE "
	default_req := "SELECT id, data FROM evt_table ORDER BY RANDOM() LIMIT 1"
	if !query.Ts.IsZero() {
		ts := query.Ts.Format("2006-01-02T15:04:05Z")
		s := fmt.Sprintf("data->'timestamp' ? '%s'", ts)
		query_req += s
		statement_cnt++
	} else {
		if !query.TsStart.IsZero() {
			ts := query.TsStart.Format("2006-01-02T15:04:05Z")
			s := fmt.Sprintf("data->>'timestamp' >= '%s'", ts)
			query_req += s
			statement_cnt++
		}
		if !query.TsEnd.IsZero() {
			if statement_cnt != 0 {
				query_req += AND_OP
			}
			ts := query.TsEnd.Format("2006-01-02T15:04:05Z")
			s := fmt.Sprintf("data->>'timestamp' <= '%s'", ts)
			query_req += s
			statement_cnt++
		}
	}

	if len(query.Resource) > 0 {
		if statement_cnt != 0 {
			query_req += AND_OP
		}
		query_req += CreateStringsConditionString(query.Resource, "component")
		statement_cnt++
	}

	if len(query.User) > 0 {
		if statement_cnt != 0 {
			query_req += AND_OP
		}
		query_req += CreateStringsConditionString(query.User, "user")
		statement_cnt++
	}

	if len(query.Operation) > 0 {
		if statement_cnt != 0 {
			query_req += AND_OP
		}
		query_req += CreateStringsConditionString(query.Operation, "op")
		statement_cnt++
	}

	if len(query.ReqId) > 0 {
		if statement_cnt != 0 {
			query_req += AND_OP
		}
		query_req += CreateNumericConditionString(query.ReqId, "req_id")
		statement_cnt++
	}

	if len(query.SessionId) > 0 {
		if statement_cnt != 0 {
			query_req += AND_OP
		}
		query_req += CreateNumericConditionString(query.SessionId, "session_id")
		statement_cnt++
	}

	if len(query.Attrs) > 0 {
		for attr, strs := range query.Attrs {
			if len(strs) > 0 {
				if statement_cnt != 0 {
					query_req += AND_OP
				}
				query_req += "("
				for i, val := range strs {
					if i != 0 {
						query_req += OR_OP
					}
					query_req += fmt.Sprintf("data->'attributes'->'%s' ? '%s'", attr, val)
				}
				query_req += ")"
				statement_cnt++
			} else {
				if statement_cnt != 0 {
					query_req += AND_OP
				}
				query_req += fmt.Sprintf("data->'attributes' ? '%s'", attr)
				statement_cnt++
			}
		}
	}
	if statement_cnt == 0 {
		return default_req, true
	} else {
		return query_req, true
	}
}

func (writer PostgresWriter) ReadEvent(query *AuditEventQuery) ([]AuditEvent, error) {
	log.Printf("Read event with query %v\n", *query)
	req, ok := CreateSelectReq(query)
	if !ok {
		log.Printf("Error while creating select req\n")
		return nil, nil
	}
	log.Printf("Use request %s\n", req)
	rows, err := writer.db.Query(req)
	if err != nil {
		log.Printf("Error while querying %v\n", err)
		return nil, err
	}

	var events []AuditEvent
	for rows.Next() {
		item := new(Item)
		err = rows.Scan(&item.ID, &item.Evt)
		if err != nil {
			log.Printf("Error while scanning row %v\n", err)
			return nil, err
		}
		events = append(events, AuditEvent(item.Evt))
	}
	return events, nil
}

func (writer PostgresWriter) Write(data []byte) error {
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

func (write PostgresWriter) Read(data []byte) ([]byte, error) {
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
