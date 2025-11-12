package main

import (
	"event/event"
	"skeleton/skeleton"
)

func main() {
	var SkeletonHandlers = map[string]skeleton.SkeletonHandler{
		"/audit/events/": event.EventHandler,
	}
	var writer event.PostgresWriter
	(&writer).InitWriter("mydb")
	eng := &event.GlobalEng
	eng.SetEngine(SkeletonHandlers, writer, "127.0.0.1:8888")
	eng.InitHandlers()
	eng.RunEngine()
}
