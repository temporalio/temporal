package sim_ctrl

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/tools/gomad/runtime"
)

var (
	testDirName = "tests"
	testPkgName = "tests_test"

	DebugMode = func(c *Controller) {
		c.conf.debug = true
	}
	ResetFiles = func(c *Controller) {
		c.resetFiles = true
	}
	FixedSeed = func(seed int64) func(c *Controller) {
		return func(c *Controller) {
			c.conf.seed = seed
		}
	}
	VerificationMode = func(c *Controller) {
		c.verificationMode = true
	}
)

type (
	Controller struct {
		resetFiles       bool
		verificationMode bool
		conf             simConfig
		simProgram       *simProgram
	}
	Option func(*Controller)
)

func NewController(options ...Option) *Controller {
	controller := &Controller{}
	controller.conf.seed = rand.Int63()
	for _, option := range options {
		option(controller)
	}
	return controller
}

func (c *Controller) RunTests(outDir string) {
	// create simulation
	if c.simProgram == nil {
		c.simProgram = createNewSimProgram(outDir, c.resetFiles)
	}

	switch {
	case c.verificationMode:
		c.verify(c.conf)

	default:
		statusCh, logsCh := c.simProgram.start(c.conf)
	loop:
		for {
			select {
			case line := <-logsCh:
				fmt.Println(line)
			case <-statusCh:
				break loop
			}
		}
	}
}

func (c *Controller) verify(baseConf simConfig) {
	// start server
	srv := newServer()
	defer func() { srv.stop() }()
	srv.start()

	baseConf.remoteAddr = srv.addr

	// start simulations
	pauseCh := make(chan any)
	exitCh := make(chan any)
	var wg sync.WaitGroup
	var clients []*simClient
	for i := 0; i < 2; i++ {
		wg.Add(1)
		conf := baseConf
		conf.remoteId = fmt.Sprintf("%v", i)
		newClient := newSimClient(conf, c.simProgram)
		clients = append(clients, newClient)

		go func(client *simClient) {
			// start
			go func() {
				client.start(pauseCh)
				exitCh <- struct{}{}
			}()

			// make sure it started
			select {
			case <-srv.clientCh:
				wg.Done()
			case <-time.After(10 * time.Second):
				fmt.Println("client #" + client.conf.remoteId + " connection timeout")
				client.printRecentLogs()
				panic("client #" + client.conf.remoteId + " never connected")
			}
		}(newClient)
	}
	wg.Wait()

	var exited int
	var paused int
	for {
		select {
		case <-time.After(2 * time.Second):
			panic("clients seem to be stuck")
		case <-pauseCh:
			paused += 1
			if paused == 2 {
				c.sync(srv, clients[0], clients[1])
				paused = 0
			}
		case <-exitCh:
			exited += 1
			if exited == 2 {
				return
			}
		}
	}
}

func (c *Controller) sync(
	srv *server,
	client1 *simClient,
	client2 *simClient,
) {
	fmt.Println("[ctrl]", "sync clients")

	log0, log1 := client1.logs, client2.logs
	for i := 0; i < max(len(log0), len(log1)); i += 1 {
		if i == len(log0) {
			fmt.Println("⚠️diff detected")
			fmt.Println("\n\t", strings.Join(log1[i:], "\n\t"))
			panic("extra logs on client #1")
		}
		if i == len(log1) {
			fmt.Println("⚠️diff detected")
			fmt.Println("\n\t", strings.Join(log0[i:], "\n\t"))
			panic("extra logs on client #0")
		}
		if log0[i] != log1[i] {
			fmt.Println("⚠️diff detected")
			startIdx := max(0, i-100)
			endIdx := i + 1
			fmt.Println("\nclient #0:\n\t", strings.Join(log0[startIdx:endIdx], "\n\t"))
			fmt.Println("\nclient #1:\n\t", strings.Join(log1[startIdx:endIdx], "\n\t"), "\n")
			panic("diff!")
		}
	}

	// tell both clients to continue
	srv.sendAllClients(sim_runtime.RemoteControlContinueCmd)
}
