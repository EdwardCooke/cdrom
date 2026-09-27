// Package config provides shared configuration loading for all cdrom
// binaries (api, worker, agent, and the gRPC service servers). Values come
// from built-in defaults, an optional YAML file (--config-file), and
// environment variables, in increasing order of precedence.
package config
