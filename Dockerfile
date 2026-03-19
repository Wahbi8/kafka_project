FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod go.sum
RUN go mod download 

COPY . .

ARG APP_NAME=producer
RUN go build -o /app/main ./${APP_NAME}

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/main .

CMD ["./main"]