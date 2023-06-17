build:
	go build -o target/cheburekdb cmd/main.go

test:
	go test -v ./...