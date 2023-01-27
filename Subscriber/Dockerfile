# Use the official Golang image
FROM golang:1.19

# Set the working directory
WORKDIR /go/src/app

# Copy the source code into the container
COPY subscriber.go .
COPY go.mod .
COPY go.sum .
COPY vendor vendor
RUN ls
CMD ["go", "run", "/go/src/app/subscriber.go"]

EXPOSE 8080