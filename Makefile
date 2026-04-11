APP     := obsi
OUTDIR  := dist
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

TARGETS := \
	$(OUTDIR)/$(APP)-linux-amd64 \
	$(OUTDIR)/$(APP)-linux-arm64 \
	$(OUTDIR)/$(APP)-darwin-amd64 \
	$(OUTDIR)/$(APP)-darwin-arm64

.PHONY: build all clean

build: $(APP)

all: $(APP) $(TARGETS)

$(APP):
	go build -ldflags '$(LDFLAGS)' -o $@ .

$(OUTDIR)/$(APP)-linux-amd64:
	GOOS=linux GOARCH=amd64 go build -ldflags '$(LDFLAGS)' -o $@ .

$(OUTDIR)/$(APP)-linux-arm64:
	GOOS=linux GOARCH=arm64 go build -ldflags '$(LDFLAGS)' -o $@ .

$(OUTDIR)/$(APP)-darwin-amd64:
	GOOS=darwin GOARCH=amd64 go build -ldflags '$(LDFLAGS)' -o $@ .

$(OUTDIR)/$(APP)-darwin-arm64:
	GOOS=darwin GOARCH=arm64 go build -ldflags '$(LDFLAGS)' -o $@ .

clean:
	rm -rf $(OUTDIR)
