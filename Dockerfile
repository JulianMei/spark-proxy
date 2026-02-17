FROM golang:1.25.3 AS builder
WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o spark-proxy main.go

# Use a minimal base image for the final container
FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/spark-proxy .
EXPOSE 8080
ENTRYPOINT ["/app/spark-proxy"]