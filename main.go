package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	var err error
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	log.SetOutput(os.Stderr)

	processPath := os.Getenv("W8Y_EXEC")
	if processPath == "" {
		log.Println("missing required W8Y_EXEC env var! must be a path to a process to execute; env and args are copied from current process")
		os.Exit(2)
	}

	redisUrl := os.Getenv("W8Y_REDIS_URL")
	if redisUrl == "" {
		redisUrl = "redis://localhost:6379"
	}

	var keyPrefix string
	keyPrefix = os.Getenv("W8Y_KEY_PREFIX")
	if keyPrefix == "" {
		log.Println("warning: empty W8Y_KEY_PREFIX env var")
	} else {
		// make sure key prefix has a ':' suffix:
		if !strings.HasSuffix(keyPrefix, ":") {
			keyPrefix += ":"
		}
	}
	log.Printf("key prefix = '%s'\n", keyPrefix)

	var keyExpirySeconds int
	if keyExpirySeconds, err = strconv.Atoi(os.Getenv("W8Y_KEY_EXPIRY_SECONDS")); err != nil {
		keyExpirySeconds = 5
		log.Printf("key expiry is %d seconds (default)\n", keyExpirySeconds)
	} else {
		log.Printf("key expiry is %d seconds\n", keyExpirySeconds)
	}

	var listKey = keyPrefix + "list"
	var procKeyPrefix = keyPrefix + "proc:"
	log.Printf("list key = '%s'\n", listKey)

	var procKey string

	// parse REDIS_URL for connection info:
	var options *redis.Options
	options, err = redis.ParseURL(redisUrl)
	if err != nil {
		log.Printf("error parsing REDIS_URL: %v\n", err)
		os.Exit(2)
	}

	// connect to redis:
	var rds *redis.Client
	rds = redis.NewClient(options)
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
	log.Printf("checking length of list '%s'\n", listKey)
	if listLen, err = rds.LLen(ctx, listKey).Result(); err != nil {
		log.Println(err)
		os.Exit(2)
	}
	log.Printf("length of list '%s' is %v\n", listKey, listLen)
	if listLen <= 0 {
		log.Println("work list is empty; no work to do")
		os.Exit(1)
	}

	var listItem string
	var procKeyExists int64 = 1

	// iterate once through the list of items:
	for i := int64(0); i < listLen; i++ {
		// pop from left side of list and atomically append to right side of list:
		if listItem, err = rds.LMove(ctx, listKey, listKey, "left", "right").Result(); err != nil {
			log.Printf("LMOVE error: %v\n", err)
			os.Exit(2)
		}

		// check for existence of processing key:
		procKey = procKeyPrefix + listItem
		if procKeyExists, err = rds.Exists(ctx, procKey).Result(); err != nil {
			log.Printf("EXISTS error: %v\n", err)
			os.Exit(2)
		}
		if procKeyExists == 0 {
			// no processor key exists for this item so let's grab it:
			break
		}

		// keep going through list items, looking for one which is not being processed:
	}

	if procKeyExists != 0 {
		// no work to do. exit and let us be restarted again after a backoff period:
		log.Printf("no available work item found in '%s'\n", listKey)
		// 0 items to return:
		fmt.Println("0")
		os.Exit(1)
	}

	// spawn process to process list item:
	args := make([]string, len(os.Args))
	copy(args, os.Args)
	args[0] = processPath
	cmd := exec.Command(args[0], args...)

	// build environment variables:
	osEnv := os.Environ()
	env := make([]string, 0, len(osEnv)+2)

	// let the process know the list item and processing key via env vars:
	env = append(env,
		fmt.Sprintf("W8Y_LIST_ITEM=%s", listItem),
		fmt.Sprintf("W8Y_PROC_KEY=%s", procKey))

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

	// start process:
	if err := cmd.Start(); err != nil {
		log.Printf("start process error: %v\n", err)
		os.Exit(2)
	}

	// run a keepalive thread in the background:
	isComplete := int32(0)
	go keepAlive(rds, procKey, time.Second*time.Duration(keyExpirySeconds), &isComplete)

	// wait for process to exit:
	err = cmd.Wait()

	// mark completed:
	atomic.StoreInt32(&isComplete, 1)

	if exitErr, ok := err.(*exec.ExitError); ok {
		// flush remaining stderr:
		os.Stderr.Write(exitErr.Stderr)
		os.Exit(exitErr.ExitCode())
	} else if err != nil {
		log.Println(err)
	}

	os.Exit(2)
}

func keepAlive(rds *redis.Client, procKey string, expiry time.Duration, isComplete *int32) {
	var err error
	log.Printf("started keepAlive thread\n")

	ctx := context.Background()

	// duration to renew is half of key expiry time:
	duration := expiry / 2

	// mark this record as being processed:
	if _, err = rds.SetEX(ctx, procKey, 1, expiry).Result(); err != nil {
		log.Printf("SET EX '%s' error: %v\n", procKey, err)
	}

	// every duration, renew the key:
	ticker := time.NewTicker(duration)
	for range ticker.C {
		if atomic.LoadInt32(isComplete) != 0 {
			break
		}

		// push out the expiry time:
		var updated bool
		if updated, err = rds.Expire(ctx, procKey, expiry).Result(); err != nil {
			log.Printf("EXPIRE '%s' error: %v\n", procKey, err)
		}
		if !updated {
			log.Printf("EXPIRE '%s' was not successfully updated\n", procKey)
		}
	}
	log.Printf("stopped keepAlive thread\n")
	ticker.Stop()
}
