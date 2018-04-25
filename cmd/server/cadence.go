// Copyright (c) 2017 Uber Technologies, Inc.
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
	"strings"

	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/tools/cassandra"

	"github.com/urfave/cli"
)

// validServices is the list of all valid cadence services
var validServices = []string{historyService, matchingService, frontendService, workerService}

// inDevelopmentServices is the list of services we want to support skipping logic on startup if config does not exist
var inDevelopmentServices = map[string]bool{workerService: true}

// main entry point for the cadence server
func main() {
	app := buildCLI()
	app.Run(os.Args)
}

// startHandler is the handler for the cli start command
func startHandler(c *cli.Context) {
	env := getEnvironment(c)
	zone := getZone(c)
	configDir := getConfigDir(c)

	log.Printf("Loading config; env=%v,zone=%v,configDir=%v\n", env, zone, configDir)

	var cfg config.Config
	err := config.Load(env, configDir, zone, &cfg)
	if err != nil {
		log.Fatal("Config file corrupted.", err)
	}
	log.Printf("config=\n%v\n", cfg.String())

	cassCfg := cfg.Cassandra
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal("Unable to get current directory")
	}
	if err := cassandra.VerifyCompatibleVersion(cassCfg, dir); err != nil {
		log.Fatal("Incompatible versions", err)
	}

	services := getServices(c)
LoadServiceLoop:
	for _, svc := range services {
		if _, ok := cfg.Services[svc]; !ok {
			if _, ok := inDevelopmentServices[svc]; len(services) > 1 && ok {
				log.Printf("Config missing for development service `%v`. Skipping to load service.\n", svc)
				continue LoadServiceLoop
			} else {
				log.Fatalf("`%v` service missing config", svc)
			}
		}
		server := newServer(svc, &cfg)
		server.Start()
	}

	select {}
}

func getEnvironment(c *cli.Context) string {
	return strings.TrimSpace(c.GlobalString("env"))
}

func getZone(c *cli.Context) string {
	return strings.TrimSpace(c.GlobalString("zone"))
}

// getServices parses the services arg from cli
// and returns a list of services to start
func getServices(c *cli.Context) []string {

	val := strings.TrimSpace(c.String("services"))
	tokens := strings.Split(val, ",")

	if len(tokens) == 0 {
		log.Fatal("list of services is empty")
	}

	for _, t := range tokens {
		if !isValidService(t) {
			log.Fatalf("invalid service `%v` in service list [%v]", t, val)
		}
	}

	return tokens
}

func isValidService(in string) bool {
	for _, s := range validServices {
		if s == in {
			return true
		}
	}
	return false
}

func getConfigDir(c *cli.Context) string {
	return constructPath(getRootDir(c), c.GlobalString("config"))
}

func getRootDir(c *cli.Context) string {
	dirpath := c.GlobalString("root")
	if len(dirpath) == 0 {
		cwd, err := os.Getwd()
		if err != nil {
			log.Fatalf("os.Getwd() failed, err=%v", err)
		}
		return cwd
	}
	return dirpath
}

func constructPath(dir string, file string) string {
	return dir + "/" + file
}

func buildCLI() *cli.App {

	app := cli.NewApp()
	app.Name = "cadence"
	app.Usage = "Cadence server"
	app.Version = "0.0.1"

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "root, r",
			Value:  ".",
			Usage:  "root directory of execution environment",
			EnvVar: config.EnvKeyRoot,
		},
		cli.StringFlag{
			Name:   "config, c",
			Value:  "config",
			Usage:  "config dir path relative to root",
			EnvVar: config.EnvKeyConfigDir,
		},
		cli.StringFlag{
			Name:   "env, e",
			Value:  "development",
			Usage:  "runtime environment",
			EnvVar: config.EnvKeyEnvironment,
		},
		cli.StringFlag{
			Name:   "zone, az",
			Value:  "",
			Usage:  "availability zone",
			EnvVar: config.EnvKeyAvailabilityZone,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:    "start",
			Aliases: []string{""},
			Usage:   "start cadence server",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "services, s",
					Value: strings.Join(validServices, ","),
					Usage: "list of services to start",
				},
			},
			Action: func(c *cli.Context) {
				startHandler(c)
			},
		},
	}

	return app

}
