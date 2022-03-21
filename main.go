package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func main() {
	var err error

	logF := setupLogging(err)
	if logF != nil {
		defer logF.Close()
	}

	processPath, workItemArgPos, workItemArgAdd, workItemKey, redisUrl, keyExpirySeconds, listKey, procKeyPrefix :=
		extractEnvVars()

	rds := connectRedis(redisUrl)
	defer func() {
		err = rds.Close()
		if err != nil {
			log.Printf("error closing redis connection: %v\n", err)
			os.Exit(2)
		}
	}()

	ctx := context.Background()

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

	var workItem string
	var procKeyExists int64 = 1
	var exitCode int

	// iterate once through the list of items:
	for i := int64(0); i < listLen; i++ {
		// pop from left side of list and atomically append to right side of list:
		if workItem, err = rds.LMove(ctx, listKey, listKey, "left", "right").Result(); err != nil {
			log.Printf("LMOVE error: %v\n", err)
			os.Exit(2)
		}

		// check for existence of processing key:
		var procKey string
		procKey = procKeyPrefix + workItem
		if procKeyExists, err = rds.Exists(ctx, procKey).Result(); err != nil {
			log.Printf("EXISTS error: %v\n", err)
			os.Exit(2)
		}

		// processing key exists:
		if procKeyExists != 0 {
			// keep going through list items, looking for one which is not being processed:
			log.Printf("work item already processing: %#v\n", workItem)
			continue
		}

		// no processing key exists for this item so let's grab it:
		log.Printf("work item available: %#v\n", workItem)

		cmd := prepareProcess(processPath, workItemKey, workItem, workItemArgPos, workItemArgAdd)

		var isComplete chan struct{}
		var done chan struct{}

		// start process:
		log.Printf("start process: %#v\n", cmd.Args)
		exitCode = 0
		if err := cmd.Start(); err != nil {
			log.Printf("start process error: %v\n", err)
			exitCode = 2
			goto maybeContinue
		}

		// run a keepalive thread in the background:
		isComplete = make(chan struct{})
		done = make(chan struct{})
		go keepAlive(rds, procKey, time.Second*time.Duration(keyExpirySeconds), isComplete, done)

		// wait for process to exit:
		err = cmd.Wait()

		// mark completed:
		close(isComplete)

		if exitErr, ok := err.(*exec.ExitError); ok {
			// flush remaining stderr:
			os.Stderr.Write(exitErr.Stderr)
			exitCode = exitErr.ExitCode()
		} else if err != nil {
			log.Println(err)
			exitCode = 2
		}

		// wait for keepAlive thread to finish:
		<-done

	maybeContinue:
		//if exitCode == 0 {
		//	continue
		//}

		break
	}

	os.Exit(exitCode)
}

func setupLogging(err error) (f *os.File) {
	var silence bool
	if os.Getenv("W8Y_LOG_SILENT") != "" {
		silence = true
	}

	var logOut io.Writer = io.Discard
	if !silence {
		if fname := os.Getenv("W8Y_LOG_FILE"); fname != "" {
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

	ts := 1
	if val, err := strconv.Atoi(os.Getenv("W8Y_LOG_TIMESTAMP")); err == nil {
		ts = val
	}

	if ts != 0 {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	} else {
		log.SetFlags(0)
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
		log.Printf("error parsing W8Y_REDIS_URL: %v\n", err)
		os.Exit(2)
	}

	// connect to redis:
	rds = redis.NewClient(options)
	return
}

func extractEnvVars() (
	processPath string,
	workItemArgPos int,
	workItemArgAdd bool,
	workItemKey string,
	redisUrl string,
	keyExpirySeconds int,
	listKey string,
	procKeyPrefix string,
) {
	var err error

	processPath = os.Getenv("W8Y_EXEC")
	if processPath == "" {
		log.Println("missing required W8Y_EXEC env var! must be a path to a process to execute; env and args are copied from current process")
		os.Exit(2)
	}
	if processPath, err = exec.LookPath(processPath); err != nil {
		log.Printf("failed to find process: %v\n", err)
		os.Exit(2)
	}

	workItemArgAdd = false
	if workItemArgPos, err = strconv.Atoi(os.Getenv("W8Y_EXEC_ARGN")); err == nil {
		workItemArgAdd = true
	}

	workItemKey = os.Getenv("W8Y_EXEC_ENVVAR")
	if workItemKey == "" {
		workItemKey = "W8Y_WORK_ITEM"
	}

	redisUrl = os.Getenv("W8Y_REDIS_URL")
	if redisUrl == "" {
		redisUrl = "redis://localhost:6379"
	}

	keyPrefix := os.Getenv("W8Y_REDIS_KEY_PREFIX")
	if keyPrefix == "" {
		log.Println("warning: empty W8Y_REDIS_KEY_PREFIX env var; using global namespace for keys")
	} else {
		// make sure key prefix has a ':' suffix:
		if !strings.HasSuffix(keyPrefix, ":") {
			keyPrefix += ":"
		}
	}
	log.Printf("key prefix = %#v\n", keyPrefix)

	if keyExpirySeconds, err = strconv.Atoi(os.Getenv("W8Y_REDIS_EXPIRY_SECONDS")); err != nil {
		keyExpirySeconds = 5
		log.Printf("key expiry is %d seconds (default)\n", keyExpirySeconds)
	} else {
		log.Printf("key expiry is %d seconds\n", keyExpirySeconds)
	}

	listKey = keyPrefix + "list"
	procKeyPrefix = keyPrefix + "proc:"
	log.Printf("list key = %#v\n", listKey)
	return
}

func prepareProcess(processPath string, workItemKey string, workItem string, argPos int, argAdd bool) *exec.Cmd {
	var args []string
	osArgs := os.Args[1:]

	// insert args if requested:
	if argAdd {
		args = make([]string, 0, len(osArgs)+1)

		// handle negative values as offset from end of args:
		if argPos < 0 {
			argPos += len(osArgs) + 1
		}
		// bounds check:
		if argPos < 0 {
			argPos = 0
		}
		if argPos > len(osArgs) {
			argPos = len(osArgs)
		}

		args = append(args, osArgs[0:argPos]...)
		args = append(args, workItem)
		args = append(args, osArgs[argPos:]...)
	} else {
		args = osArgs
	}

	// create a command with path and arguments:
	cmd := exec.Command(processPath, args...)

	// build environment variables:
	osEnv := os.Environ()
	env := make([]string, 0, len(osEnv)+2)

	// let the process know the work item and processing key via env vars:
	env = append(env, fmt.Sprintf("%s=%s", workItemKey, workItem))

	// copy in env vars, filtering out "W8Y_" prefixed keys:
	for _, kv := range osEnv {
		if strings.HasPrefix(kv, "W8Y_") {
			continue
		}

		env = append(env, kv)
	}

	cmd.Env = env

	// redirect standard file handles:
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	return cmd
}

func keepAlive(rds *redis.Client, procKey string, expiry time.Duration, isComplete <-chan struct{}, done chan<- struct{}) {
	var err error

	ctx := context.Background()

	// duration to renew is half of key expiry time:
	duration := expiry / 2

	// mark this record as being processed:
	if _, err = rds.SetEX(ctx, procKey, 1, expiry).Result(); err != nil {
		log.Printf("SET EX '%s' error: %v\n", procKey, err)
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
				log.Printf("EXPIRE '%s' error: %v\n", procKey, err)
			}
			if !updated {
				log.Printf("EXPIRE '%s' was not successfully updated\n", procKey)
			}
		}
	}

	//log.Printf("stopped keepAlive thread\n")
	ticker.Stop()

	close(done)
}
