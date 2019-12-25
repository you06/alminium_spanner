// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
)

// Spanner struct
type Spanner struct {
}

// Config struct
type Config struct {
	// basic configuration
	Mode        string `toml:"mode"`
	Database    string `toml:"database"`
	Host        string `toml:"host"`
	Port        int    `toml:"port"`
	Username    string `toml:"username"`
	Password    string `toml:"password"`
	// task configuration
	WorkerName     string `toml:"worker-name"`
	RunWorks       string `toml:"run-works"`
	Concurrency    int    `toml:"concurrency"`
	TableName      string `toml:"table-name"`
	BenchmarkCount int    `toml:"benchmark-count"`
}

var initConfig = Config{
	Mode: "single",
	Database: "alminium",
}

// Init get default Config
func Init() *Config {
	return initConfig.Copy()
}

// Load config from file 
func (c *Config) Load(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.WithStack(err)
}

// Copy Config struct
func (c *Config) Copy() *Config {
	cp := *c
	return &cp
}
