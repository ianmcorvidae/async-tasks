FROM golang:1.11-alpine

RUN apk add --no-cache git
RUN go get -u github.com/jstemmer/go-junit-report

COPY . /go/src/github.com/cyverse-de/async-tasks
ENV CGO_ENABLED=0
RUN go install -v github.com/cyverse-de/async-tasks

ENTRYPOINT ["async-tasks"]
CMD ["--help"]
EXPOSE 60000

ARG git_commit=unknown
ARG version="1.0.0"
ARG descriptive_version=unknown

LABEL org.cyverse.git-ref="$git_commit"
LABEL org.cyverse.version="$version"
LABEL org.cyverse.descriptive-version="$descriptive_version"
LABEL org.label-schema.vcs-ref="$git_commit"
LABEL org.label-schema.vcs-url="https://github.com/cyverse-de/async-tasks"
LABEL org.label-schema.version="$descriptive_version"
