// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"log"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/urfave/cli"

	"github.com/temporalio/temporal/canary"
	"github.com/temporalio/temporal/common/service/config"
)

func startHandler(c *cli.Context) {
	env := getEnvironment(c)
	zone := getZone(c)
	configDir := getConfigDir(c)

	log.Printf("Loading config; env=%v,zone=%v,configDir=%v\n", env, zone, configDir)

	var cfg canary.Config
	if err := config.Load(env, configDir, zone, &cfg); err != nil {
		log.Fatal("Failed to load config file: ", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatal("Invalid config: ", err)
	}

	if cfg.Canary.PProf.Port != 0 {
		canary.StartPProf(cfg.Canary.PProf.Port)
	}

	canary, err := canary.NewCanaryRunner(&cfg)
	if err != nil {
		log.Fatal("Failed to initialize canary: ", err)
	}

	if err := canary.Run(); err != nil {
		log.Fatal("Failed to run canary: ", err)
	}

	dumpMemoryProfile(c)
}

func dumpMemoryProfile(c *cli.Context) {
	if !c.GlobalIsSet("memprofile") {
		return
	}

	memprofile := c.GlobalString("memprofile")
	log.Printf("Writing memory profile to: %s\n", memprofile)
	f, err := os.Create(memprofile)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer f.Close() // error handling omitted for example
	runtime.GC()    // get up-to-date statistics
	if err = pprof.WriteHeapProfile(f); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}

func getRootDir(c *cli.Context) string {
	rootDir := c.GlobalString("root")
	if len(rootDir) == 0 {
		var err error
		if rootDir, err = os.Getwd(); err != nil {
			rootDir = "."
		}
	}
	return rootDir
}

func getConfigDir(c *cli.Context) string {
	rootDir := getRootDir(c)
	configDir := c.GlobalString("config")
	return path.Join(rootDir, configDir)
}

func getEnvironment(c *cli.Context) string {
	return strings.TrimSpace(c.GlobalString("env"))
}

func getZone(c *cli.Context) string {
	return strings.TrimSpace(c.GlobalString("zone"))
}

func buildCLI() *cli.App {
	app := cli.NewApp()
	app.Name = "canary"
	app.Usage = "Temporal canary"
	app.Version = "0.0.1"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "root, r",
			Value:  ".",
			Usage:  "root directory of execution environment",
			EnvVar: canary.EnvKeyRoot,
		},
		cli.StringFlag{
			Name:   "config, c",
			Value:  "config/canary",
			Usage:  "config dir path relative to root",
			EnvVar: canary.EnvKeyConfigDir,
		},
		cli.StringFlag{
			Name:   "env, e",
			Value:  "development",
			Usage:  "runtime environment",
			EnvVar: canary.EnvKeyEnvironment,
		},
		cli.StringFlag{
			Name:   "zone, az",
			Value:  "",
			Usage:  "availability zone",
			EnvVar: canary.EnvKeyAvailabilityZone,
		},
		cli.StringFlag{
			Name:  "memprofile, mp",
			Value: "",
			Usage: "memprofile",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:  "start",
			Usage: "start temporal canary",
			Action: func(c *cli.Context) {
				startHandler(c)
			},
		},
	}

	return app
}

func main() {
	app := buildCLI()
	app.Run(os.Args)
}
