FROM golang:1.27 AS build
WORKDIR /go/src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/mongobetween .


FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /go/src/bin/mongobetween /mongobetween
ENTRYPOINT ["/mongobetween"]
