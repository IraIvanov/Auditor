/* This package describes audit event as structure */
package event

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"
)

type AuditEvent struct {
	Timestamp time.Time              `json:"timestamp"`
	User      string                 `json:"user"`
	Resource  string                 `json:"component,omitempty"`
	Operation string                 `json:"op"`
	SessionId uint64                 `json:"session_id,omitempty"`
	RequestId uint64                 `json:"req_id,omitempty"`
	Response  map[string]interface{} `json:"res,omitempty"`
	Attrs     map[string]string      `json:"attributes,omitempty"`
}

func DecodeEvent(Data []byte) (*AuditEvent, error) {
	var evt AuditEvent
	err := json.Unmarshal(Data, &evt)
	if err != nil {
		return nil, err
	}
	return &evt, nil
}

func EncodeEvent(Evt *AuditEvent) ([]byte, error) {
	payload, err := json.Marshal(*Evt)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

type AuditEventQuery struct {
	TsStart   time.Time           `json:"ev_ts_start,omitempty"`
	TsEnd     time.Time           `json:"ev_ts_end,omitempty"`
	Ts        time.Time           `json:"ev_ts,omitempty"`
	Resource  []string            `json:"ev_component,omitempty"`
	User      []string            `json:"ev_user,omitempty"`
	Operation []string            `json:"ev_op,omitempty"`
	SessionId []uint64            `json:"ev_session_id,omitempty"`
	ReqId     []uint64            `json:"ev_req_id,omitempty"`
	Attrs     map[string][]string `json:"attrs,omitempty"`
}

func DecodeEventQuery(Data []byte) (*AuditEventQuery, error) {
	var query AuditEventQuery
	err := json.Unmarshal(Data, &query)
	if err != nil {
		return nil, err
	}
	return &query, nil
}

func EncodeEventQuery(query *AuditEventQuery) ([]byte, error) {
	payload, err := json.Marshal(*query)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func ConvertStrListToInt(l []string) ([]uint64, error) {
	res := make([]uint64, 0)
	strs := make([]string, 0)
	for _, str := range l {
		strs = append(strs, strings.Split(str, ",")...)
	}

	for _, el := range strs {
		num, err := strconv.Atoi(el)
		if err != nil {
			log.Printf("Error while converting str to int %v\n", err)
			return nil, err
		}
		res = append(res, uint64(num))
	}
	return res, nil
}

/* TODO: change errors to proper ones */
func ConvertMapToEventQuery(params map[string][]string) (*AuditEventQuery, error) {
	var query AuditEventQuery
	query.Attrs = make(map[string][]string)
	for param := range params {
		switch param {
		case "ev_ts_start":
			if len(params[param]) > 1 {
				log.Printf("Error while %s too many params\n", param)
				return nil, &json.InvalidUnmarshalError{}
			}
			start, err := time.Parse(time.RFC3339, params[param][0])
			if err != nil {
				log.Printf("Error while parsing time string %s:%s\n", param, params[param][0])
				return nil, err
			}
			if _, ok := params["ev_ts"]; ok {
				log.Printf("Error, can't use ev_ts and ev_ts_start at the same time")
				return nil, &json.InvalidUnmarshalError{}
			}
			query.TsStart = start
		case "ev_ts_end":
			if len(params[param]) > 1 {
				log.Printf("Error while %s too many params\n", param)
				return nil, &json.InvalidUnmarshalError{}
			}
			end, err := time.Parse(time.RFC3339, params[param][0])
			if err != nil {
				log.Printf("Error while parsing time string %s:%s\n", param, params[param][0])
				return nil, err
			}
			if _, ok := params["ev_ts"]; ok {
				log.Printf("Error, can't use ev_ts and ev_ts_end at the same time")
				return nil, &json.InvalidUnmarshalError{}
			}
			query.TsEnd = end
		case "ev_ts":
			if len(params[param]) > 1 {
				log.Printf("Error while %s too many params\n", param)
				return nil, &json.InvalidUnmarshalError{}
			}
			ts, err := time.Parse(time.RFC3339, params[param][0])
			if err != nil {
				log.Printf("Error while parsing time string %s:%s\n", param, params[param][0])
				return nil, err
			}
			if _, ok := params["ev_ts_start"]; ok {
				log.Printf("Error, can't use ev_ts and ev_ts_start at the same time\n")
				return nil, &json.InvalidUnmarshalError{}
			}
			if _, ok := params["ev_ts_end"]; ok {
				log.Printf("Error, can't use ev_ts and ev_ts_end at the same time\n")
				return nil, &json.InvalidUnmarshalError{}
			}
			query.Ts = ts
		case "ev_component":
			query.Resource = make([]string, 0)
			for _, str := range params[param] {
				query.Resource = append(query.Resource, strings.Split(str, ",")...)
			}
		case "ev_user":
			query.User = make([]string, 0)
			for _, str := range params[param] {
				query.User = append(query.User, strings.Split(str, ",")...)
			}
		case "ev_op":
			query.Operation = make([]string, 0)
			for _, str := range params[param] {
				query.Operation = append(query.Operation, strings.Split(str, ",")...)
			}
		case "ev_session_id":
			sessions, err := ConvertStrListToInt(params[param])
			if err != nil {
				log.Printf("Error, can't parse %s:%v\n", "ev_session_id", err)
				return nil, &json.InvalidUnmarshalError{}
			}
			query.SessionId = sessions
		case "ev_req_id":
			reqs, err := ConvertStrListToInt(params[param])
			if err != nil {
				log.Printf("Error, can't parse %s:%v\n", "ev_req_id", err)
				return nil, &json.InvalidUnmarshalError{}
			}
			query.ReqId = reqs
		default:
			/* every unknown parameter serves as key in attributes map */
			log.Printf("Assign %v %v %v", params[param], query.Attrs, param)
			query.Attrs[param] = make([]string, 0)
			for _, str := range params[param] {
				query.Attrs[param] = append(query.Attrs[param], strings.Split(str, ",")...)
			}
		}
	}
	return &query, nil
}

type EventWriter interface {
	WriteEvent(*AuditEvent) error                     /* write particular event */
	ReadEvent(*AuditEventQuery) ([]AuditEvent, error) /* read event, with fields*/
}
