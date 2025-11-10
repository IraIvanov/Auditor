/* This describes http server, http server engine and storage interface */
package skeleton

import (
	"log"
	"net/http"
)

type SkeletonHandler func(w http.ResponseWriter, r *http.Request)

type Storage interface {
	/* should contain methods write and read entry */
	Write([]byte) error
	Read([]byte) ([]byte, error) /* read accepts file struct or query struct as bytes array */
}

type SkeletonEngine struct {
	handlers   map[string]SkeletonHandler
	storDevice Storage
	endpoint   string
}

func (eng *SkeletonEngine) InitHandlers() {
	for resource := range eng.handlers {
		log.Printf("Add handler for %s\n", resource)
		http.HandleFunc(resource, eng.handlers[resource])
	}
}

func (eng *SkeletonEngine) SetEngine(handlers map[string]SkeletonHandler, device Storage, endpoint string) {
	eng.handlers = handlers
	eng.storDevice = device
	eng.endpoint = endpoint
	log.Printf("%v %v %s\n", eng.handlers, eng.storDevice, eng.endpoint)
}

func (eng *SkeletonEngine) RunEngine() {
	log.Printf("Start serving requests %s\n", eng.endpoint)
	log.Fatal(http.ListenAndServe(eng.endpoint, nil))
}

func (eng SkeletonEngine) WriteData(data []byte) error {
	return eng.storDevice.Write(data)
}

func (eng SkeletonEngine) ReadData(data []byte) ([]byte, error) {
	return eng.storDevice.Read(data)
}
