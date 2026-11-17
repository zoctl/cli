GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS ?= -s -w -X main.version=$(VERSION)

# Install dir: default to ~/.local/bin; allow PREFIX or BINDIR overrides.
ifdef PREFIX
BINDIR ?= $(PREFIX)/bin
else
BINDIR ?= $(HOME)/.local/bin
endif

.PHONY: all help build test vet fmt check install uninstall clean

all: build

help:
	@echo "zoctl make targets:"
	@echo "  build      build stripped binary (default)"
	@echo "  test       run tests with the race detector"
	@echo "  vet        run go vet"
	@echo "  check      gofmt, vet, test and cgo-free builds for every release target"
	@echo "  install    install to \$$(BINDIR) [$(BINDIR)]"
	@echo "  uninstall  remove installed binary"
	@echo "  clean      remove build output"
	@echo ""
	@echo "variables: PREFIX (forces \$$PREFIX/bin), BINDIR, DESTDIR, GO [$(GO)]"

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o zoctl ./cmd/zoctl

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

fmt:
	@files=$$(gofmt -l .) || exit 1; if [ -n "$$files" ]; then printf '%s\n' "$$files"; exit 1; fi

check: fmt vet test
	sh -n install.sh
	@for target in linux/amd64 linux/arm64 darwin/arm64 darwin/amd64 windows/amd64; do \
	  GOOS=$${target%/*} GOARCH=$${target#*/} CGO_ENABLED=0 $(GO) build -trimpath -o /dev/null ./cmd/zoctl || exit 1; \
	done

install: build
	@dir=$(DESTDIR)$(BINDIR); \
	while [ ! -e "$$dir" ]; do dir=$$(dirname "$$dir"); done; \
	if [ ! -w "$$dir" ]; then \
		echo "error: no write access to $$dir"; \
		echo "  try:  sudo make install"; \
		echo "  or:   make install PREFIX=\$$HOME/.local  (then add ~/.local/bin to PATH)"; \
		exit 1; \
	fi
	install -d $(DESTDIR)$(BINDIR)
	install -m 0755 zoctl $(DESTDIR)$(BINDIR)/zoctl
	@echo "installed $(BINDIR)/zoctl"
	@case ":$$PATH:" in *:"$(BINDIR)":*) ;; \
	*) echo "note: $(BINDIR) is not on your PATH" ;; esac

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/zoctl

clean:
	rm -f zoctl
