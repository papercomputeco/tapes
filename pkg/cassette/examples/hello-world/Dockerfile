# The hello-world cassette image.
#
# The build context is this directory, not the repository root: the cassette is
# its own module and depends on nothing of tapes'. That is the point — an image
# that could only be built from inside the tapes tree would not be evidence
# that a third party can build a cassette.
#
#   docker build -t tapes/hello-world-cassette:0.0.1 \
#     pkg/cassette/examples/hello-world
FROM golang:1.26 AS build

WORKDIR /src

# Dependencies first, so editing the cassette does not re-resolve the module
# graph on every rebuild.
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

# CGO off gives a static binary, which is what lets the final stage be
# distroless: nothing in this cassette needs libc.
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/hello-world .

FROM gcr.io/distroless/static-debian12:nonroot

# Matches cassette.port in cassette.toml and in the OpenAPI document's
# x-tapes-cassette metadata. The three are declared separately on purpose: this
# line is what the image does, and the other two are metadata — one for whoever
# deploys the image, one for core. It is not 8080 because tapes' own proxy is.
EXPOSE 9999

# Bind the wildcard address inside the container's own network namespace. The
# deployment publishes it to 127.0.0.1 on the host, so this is not an exposure —
# binding loopback here would make the published port unreachable.
ENV CASSETTE_LISTEN=0.0.0.0:9999

COPY --from=build /out/hello-world /usr/local/bin/hello-world

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/hello-world"]
