// terraform-provider-basin — Plugin Protocol v6 entrypoint.
//
// Built with -ldflags "-X .../internal/provider.Version=…
// -X .../internal/apiclient.UserAgentVersion=…" at release time.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"
	"github.com/bas-in/terraform-provider-basin/internal/provider"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
)

// version is overridden via -ldflags at release time.
var version = "dev"

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "set to true to run the provider with support for debuggers like delve")
	flag.Parse()

	apiclient.UserAgentVersion = version

	opts := providerserver.ServeOpts{
		Address: "registry.terraform.io/bas-in/basin",
		Debug:   debug,
	}
	if err := providerserver.Serve(context.Background(), provider.New(version), opts); err != nil {
		log.Fatal(err.Error())
	}
}
