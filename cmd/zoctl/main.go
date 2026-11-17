// Command zoctl is the zoctl console from a terminal.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/zoctl/cli/internal/cli"
	"github.com/zoctl/cli/internal/creds"
)

// version is set at build time: -ldflags "-X main.version=1.2.3".
var version = "dev"

func main() {
	store, err := creds.Default(os.Getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "zoctl: there's no configuration folder to keep a session in: %v\n", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	app := &cli.App{
		Version: version,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Getenv:  os.Getenv,
		HTTP:    &http.Client{Timeout: time.Minute},
		Store:   store,
		Now:     time.Now,
		Open:    cli.OpenBrowser,
		Sleep:   cli.Sleep,
	}
	code := app.Run(ctx, os.Args[1:])
	stop()
	os.Exit(code)
}
