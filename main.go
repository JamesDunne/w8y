package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	var err error
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	log.SetOutput(os.Stderr)

	redisUrl := os.Getenv("REDIS_URL")
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
		log.Printf("key expiry in %d seconds (default)\n", keyExpirySeconds)
	} else {
		log.Printf("key expiry in %d seconds\n", keyExpirySeconds)
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

	// mark this record as being processed; intended to be a keep-alive every N seconds:
	if _, err = rds.SetEX(ctx, procKey, 1, time.Second*time.Duration(keyExpirySeconds)).Result(); err != nil {
		log.Printf("SET EX error: %v\n", err)
		os.Exit(2)
	}

	// 2 items to return; listItem and procKey:
	fmt.Println("2")
	fmt.Println(listItem)
	fmt.Println(procKey)
	os.Exit(0)
}
