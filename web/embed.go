// Package web embeds the portal's static build (D-N4: the SvelteKit SPA is
// served as static files from the coordinator binary). A placeholder build
// is committed so the binary compiles before `make web`.
package web

import "embed"

// all: is required so the _app asset tree is embedded (embed skips
// names starting with '.' or '_' without the all: prefix).
//
//go:embed all:build
var FS embed.FS
