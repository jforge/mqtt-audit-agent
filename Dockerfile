# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/audit-agent ./cmd/audit-agent

FROM alpine:3.22
RUN adduser -D -H app
USER app
WORKDIR /app
COPY --from=build /out/audit-agent /app/audit-agent
EXPOSE 8080
ENTRYPOINT ["/app/audit-agent"]