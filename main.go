package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"os"
	"time"
)

func main() {
	var err error
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)

	// parse REDIS_URL for connection info:
	var options *redis.Options
	redisUrl := os.Getenv("REDIS_URL")
	if redisUrl == "" {
		redisUrl = "redis://localhost:6379"
	}

	options, err = redis.ParseURL(redisUrl)
	if err != nil {
		log.Fatalln(err)
	}

	var rds *redis.Client
	rds = redis.NewClient(options)
	defer func() {
		err = rds.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}()

	ctx := context.Background()

	var keyPrefix = "tenants"
	var keyList = keyPrefix + ":list"
	var keyProcessing string

	var tenantId string

	// check list length up front so we don't end up circling around the list forever. the list length may change during
	// iteration but this is okay since we can always restart and pick up the new list size.
	var listLen int64
	if listLen, err = rds.LLen(ctx, keyList).Result(); err != nil {
		log.Fatalln(err)
	}

	var processingKeyExists int64

	// iterate through the list of items to process for:
	for i := int64(0); i < listLen; i++ {
		// pop from left side of list and atomically append to right side of list:
		if tenantId, err = rds.LMove(ctx, keyList, keyList, "left", "right").Result(); err != nil {
			log.Fatalln(err)
		}

		// check processing marker:
		keyProcessing = fmt.Sprintf("%s:%s", keyPrefix, tenantId)
		if processingKeyExists, err = rds.Exists(ctx, keyProcessing).Result(); err != nil {
			log.Fatalln(err)
		}
		if processingKeyExists == 0 {
			// no processor key exists for this item so let's grab it:
			break
		}

		// keep going through list items, looking for one which is not being processed:
	}

	if processingKeyExists != 0 {
		// no work to do. exit and let us be restarted again after a backoff period:
		return
	}

	// mark this record as being processed; intended to be a keep-alive every N seconds:
	if _, err = rds.SetEX(ctx, keyProcessing, 1, time.Second*5).Result(); err != nil {
		log.Fatalln(err)
	}

	fmt.Println(tenantId)
}
