# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH

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