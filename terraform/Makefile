# terraform-provider-basin — Makefile
#
# `make build`   produces a local binary in the repo root.
# `make install` symlinks it into ~/.terraform.d/plugins/... so that
# `terraform init` resolves the provider locally for development.
# `make test`    runs unit + lifecycle tests (no TF_ACC needed).
# `make testacc` runs the full Terraform acceptance suite (TF_ACC=1).

BINARY      = terraform-provider-basin
VERSION    ?= 0.1.0
NAMESPACE   = bas-in
NAME        = basin
HOSTNAME    = registry.terraform.io

# Detect host OS/arch the same way the registry does.
OS_ARCH    ?= $(shell go env GOOS)_$(shell go env GOARCH)

PLUGIN_DIR  = $(HOME)/.terraform.d/plugins/$(HOSTNAME)/$(NAMESPACE)/$(NAME)/$(VERSION)/$(OS_ARCH)

LDFLAGS     = -X github.com/bas-in/terraform-provider-basin/internal/provider.Version=$(VERSION) \
              -X github.com/bas-in/terraform-provider-basin/internal/apiclient.UserAgentVersion=$(VERSION)

.PHONY: build install test testacc clean fmt vet

build:
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) ./

install: build
	mkdir -p $(PLUGIN_DIR)
	cp $(BINARY) $(PLUGIN_DIR)/$(BINARY)_v$(VERSION)

test:
	go test ./...

testacc:
	TF_ACC=1 go test ./... -v -timeout 120m

fmt:
	go fmt ./...

vet:
	go vet ./...

clean:
	rm -f $(BINARY)
