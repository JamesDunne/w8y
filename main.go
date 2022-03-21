package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jessevdk/go-flags"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Options struct {
	RedisUrl        string `short:"u" long:"redis-url" default:"redis://localhost:6379" description:"Redis URL to connect to"`
	KeyPrefix       string `short:"k" long:"key-prefix" description:"Redis prefix for all keys"`
	KeyExpiry       int    `short:"x" long:"key-expiry" default:"5" description:"Redis processing key expiry in seconds"`
	Quiet           bool   `short:"q" long:"quiet" description:"Silence output of w8y to capture pure stdout,stderr of spawned executable"`
	LogFile         string `short:"f" long:"log-file" description:"Log to file"`
	NoLogTimestamps bool   `short:"t" long:"no-log-timestamps" description:"Disable inclusion of timestamps in log lines"`
	EnvVar          string `short:"e" long:"env-var" description:"Environment variable name to set work item to"`
	Args            struct {
		Executable string   `positional-arg-name:"executable"`
		Rest       []string `positional-arg-name:"args" description:"Arguments to pass to executable; use {} as a placeholder for work item value"`
	} `positional-args:"true" required:"true"`
}

func main() {
	var err error

	opts := &Options{}
	_, err = flags.NewParser(opts, flags.HelpFlag).Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}

	logF := setupLogging(opts)
	if logF != nil {
		defer logF.Close()
	}

	//fmt.Printf("%#v\n", opts)

	validateOptions(opts)

	rds := connectRedis(opts.RedisUrl)
	defer func() {
		err = rds.Close()
		if err != nil {
			log.Printf("error closing redis connection: %v\n", err)
			os.Exit(2)
		}
	}()

	ctx := context.Background()

	listKey := opts.KeyPrefix + "list"
	procKeyPrefix := opts.KeyPrefix + "proc:"
	log.Printf("list key = %#v\n", listKey)

	// check list length up front so we don't end up circling around the list forever. the list length may change during
	// iteration but this is okay since we can always restart and pick up the new list size.
	var listLen int64
	log.Printf("checking length of %#v\n", listKey)
	if listLen, err = rds.LLen(ctx, listKey).Result(); err != nil {
		log.Println(err)
		os.Exit(2)
	}
	log.Printf("length of %#v is %v\n", listKey, listLen)
	if listLen <= 0 {
		log.Println("empty; no work to do")
		os.Exit(0)
	}

	var exitCode int

	// iterate once through the list of items:
	for i := int64(0); i < listLen; i++ {
		var shouldContinue bool

		shouldContinue, exitCode, err = iterateList(ctx, rds, listKey, procKeyPrefix, opts)

		if err != nil {
			break
		}
		if !shouldContinue {
			break
		}
	}

	os.Exit(exitCode)
}

func iterateList(ctx context.Context, rds *redis.Client, listKey string, procKeyPrefix string, opts *Options) (shouldContinue bool, exitCode int, err error) {
	var procKeyExists int64 = 1

	shouldContinue = false
	exitCode = -1

	// pop from left side of list and atomically append to right side of list:
	var workItem string
	if workItem, err = rds.LMove(ctx, listKey, listKey, "left", "right").Result(); err != nil {
		log.Printf("LMOVE error: %v\n", err)
		return
	}

	// check for existence of processing key:
	var procKey string
	procKey = procKeyPrefix + workItem
	if procKeyExists, err = rds.Exists(ctx, procKey).Result(); err != nil {
		log.Printf("EXISTS error: %v\n", err)
		return
	}

	// processing key exists:
	if procKeyExists != 0 {
		// keep going through list items, looking for one which is not being processed:
		log.Printf("work item already processing: %#v\n", workItem)
		shouldContinue = true
		return
	}

	// no processing key exists for this item so let's grab it:
	log.Printf("work item available: %#v\n", workItem)

	// run a keepalive thread in the background:
	isComplete := make(chan struct{})
	done := make(chan struct{})
	go keepAlive(rds, procKey, time.Second*time.Duration(opts.KeyExpiry), isComplete, done)

	// start process:
	cmd := prepareProcess(opts, workItem)
	log.Printf("start process: %#v\n", cmd.Args)
	if err = cmd.Start(); err != nil {
		log.Printf("start process error: %v\n", err)
		return
	}

	// wait for process to exit:
	err = cmd.Wait()

	// mark completed:
	close(isComplete)

	if exitErr, ok := err.(*exec.ExitError); ok {
		// flush remaining stderr:
		os.Stderr.Write(exitErr.Stderr)
		exitCode = exitErr.ExitCode()
		err = nil
	} else if err != nil {
		log.Printf("exit process error: %v\n", err)
		exitCode = -1
	} else {
		exitCode = 0
	}

	// wait for keepAlive thread to finish:
	<-done

	shouldContinue = false
	return
}

func setupLogging(opts *Options) (f *os.File) {
	silence := opts.Quiet

	var logOut io.Writer = io.Discard
	if !silence {
		if fname := opts.LogFile; fname != "" {
			var err error
			f, err = os.OpenFile(fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
			if err != nil {
				fmt.Printf("failed to open file for writing: %v\n", fname)
				os.Exit(2)
			}
			logOut = f
		} else {
			logOut = os.Stderr
		}
	}

	if opts.NoLogTimestamps {
		log.SetFlags(0)
	} else {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	}

	log.SetPrefix("w8y: ")
	log.SetOutput(logOut)

	return
}

func connectRedis(redisUrl string) (rds *redis.Client) {
	var err error

	// parse REDIS_URL for connection info:
	var options *redis.Options
	options, err = redis.ParseURL(redisUrl)
	if err != nil {
		log.Printf("error parsing redis URL: %v\n", err)
		os.Exit(2)
	}

	// connect to redis:
	rds = redis.NewClient(options)
	return
}

func validateOptions(opts *Options) {
	var err error

	processPath := &opts.Args.Executable
	// *processPath is guaranteed to be non-empty here thanks to flags required:true
	if *processPath, err = exec.LookPath(*processPath); err != nil {
		log.Printf("failed to find process: %v\n", err)
		os.Exit(2)
	}

	if opts.RedisUrl == "" {
		opts.RedisUrl = "redis://localhost:6379"
	}

	if opts.KeyPrefix == "" {
		log.Println("warning: empty key-prefix; using global namespace for keys")
	} else {
		// make sure key prefix has a ':' suffix:
		if !strings.HasSuffix(opts.KeyPrefix, ":") {
			opts.KeyPrefix += ":"
		}
	}

	log.Printf("key prefix = %#v\n", opts.KeyPrefix)
	log.Printf("key expiry is %d seconds\n", opts.KeyExpiry)

	return
}

func prepareProcess(opts *Options, workItem string) *exec.Cmd {
	// build arguments to the executable:
	osArgs := opts.Args.Rest
	args := make([]string, 0, len(osArgs))
	for _, arg := range osArgs {
		// replace {} token with the work item:
		if arg == "{}" {
			arg = workItem
		}
		args = append(args, arg)
	}

	// create a command with path and arguments:
	cmd := exec.Command(opts.Args.Executable, args...)

	// build environment variables:
	osEnv := os.Environ()
	var env []string
	if opts.EnvVar != "" {
		// let the process know the work item and processing key via env vars:
		env = make([]string, len(osEnv)+1)
		env[0] = fmt.Sprintf("%s=%s", opts.EnvVar, workItem)
		// copy existing env vars:
		copy(env[1:], osEnv)
	} else {
		// copy existing env vars:
		env = make([]string, len(osEnv))
		copy(env, osEnv)
	}

	cmd.Env = env

	// redirect standard file handles:
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd
}

func keepAlive(rds *redis.Client, procKey string, expiry time.Duration, isComplete <-chan struct{}, done chan<- struct{}) {
	var err error

	ctx := context.Background()

	// duration to renew is half of key expiry time:
	duration := expiry / 2

	// mark this record as being processed:
	if _, err = rds.SetEX(ctx, procKey, 1, expiry).Result(); err != nil {
		log.Printf("SET EX %#v error: %v\n", procKey, err)
	}

	// every duration, renew the key:
	ticker := time.NewTicker(duration)

loop:
	for {
		select {
		case <-isComplete:
			break loop
		case <-ticker.C:
			// push out the expiry time:
			var updated bool
			if updated, err = rds.Expire(ctx, procKey, expiry).Result(); err != nil {
				log.Printf("EXPIRE %#v error: %v\n", procKey, err)
			} else if !updated {
				log.Printf("EXPIRE %#v was not successful\n", procKey)
			}
		}
	}

	//log.Printf("stopped keepAlive thread\n")
	ticker.Stop()

	var ok int64
	if ok, err = rds.Del(ctx, procKey).Result(); err != nil {
		log.Printf("DEL %#v error: %v\n", procKey, err)
	} else if ok == 0 {
		log.Printf("DEL %#v was not successful: %v\n", procKey)
	}

	close(done)
}
