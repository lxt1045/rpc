package filesystem

import "embed"

// Static is a filesystem to serve static files
//go:embed static
var Static embed.FS
