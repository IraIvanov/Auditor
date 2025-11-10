package event

import (
	"io"
	"log"
	"net/http"

	"skeleton/skeleton"
)

var GlobalEng skeleton.SkeletonEngine

func EventHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		/* get content from request */
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()
		err = GlobalEng.WriteData(bodyBytes)
		if err != nil {
			log.Printf("Error while writing data %v\n", err)
			http.Error(w, "Error writing request data", http.StatusBadGateway)
		}
	case http.MethodGet:
		/* get query params and init event query struct */
		/* convert struct to json and use it to read data */
		params := r.URL.Query()
		query, err := ConvertMapToEventQuery(params)
		if err != nil {
			log.Printf("Error while parsing query params %v\n", err)
			http.Error(w, "Error while parsing query params", http.StatusBadRequest)
			return
		}
		payload, err := EncodeEventQuery(query)
		if err != nil {
			log.Printf("Error while converting query to json %v\n", err)
			http.Error(w, "Error while converting query to json", http.StatusBadGateway)
			return
		}
		resp, err := GlobalEng.ReadData(payload)
		if err != nil {
			log.Printf("Error while reading data %v\n", err)
			http.Error(w, "Error while reading data", http.StatusBadRequest)
			return
		}
		_, err = w.Write(resp)
		if err != nil {
			log.Printf("Error while writing response %v\n", err)
			http.Error(w, "Error while writing response", http.StatusBadGateway)
			return
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("Error, method not allowed %s\n", r.Method)
		return
	}
}
