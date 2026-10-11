// Package config provides shared configuration loading for all cdrom
// binaries (api, worker, agent, and the gRPC service servers).
//
// Configuration is resolved in three layers, in increasing order of
// precedence:
//
//  1. built-in defaults, suitable for a local development setup where
//     every service runs on localhost;
//  2. an optional YAML file, passed with the --config-file command-line
//     flag (see ParseFlags);
//  3. environment variables, which override both.
package config
