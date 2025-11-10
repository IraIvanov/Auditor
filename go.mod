module auditor/main

go 1.23.3

replace skeleton/skeleton => ./skeleton

replace event/event => ./event

require (
	event/event v0.0.0-00010101000000-000000000000
	skeleton/skeleton v0.0.0-00010101000000-000000000000
)

require github.com/lib/pq v1.10.9 // indirect
