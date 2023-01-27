# Use the official Golang image
FROM golang:1.19

# Set the working directory
WORKDIR /go/src/app

# Copy the source code into the container
COPY publisher.go .
COPY go.mod .
COPY go.sum .
RUN go mod download

CMD ["go", "run", "/go/src/app/publisher.go"]

EXPOSE 8080