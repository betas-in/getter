package getter

import (
	"context"
	"strconv"
	"time"

	"github.com/betas-in/logger"
	"github.com/betas-in/rediscache"
)

var (
	keyRateLimit = "rate"
	separator    = "."
)

// RateLimits ...
type RateLimits struct {
	log   *logger.Logger
	list  []RateLimit
	cache rediscache.Cache
}

func (r *RateLimits) addRateLimit(host, bucket string, count int64) error {
	if host == "" {
		return &Error{"host should not be empty"}
	}
	if bucket == "" {
		return &Error{"bucket should not be empty"}
	}
	if count == 0 {
		return &Error{"count should not be empty"}
	}
	bucketTime, err := time.ParseDuration(bucket)
	if err != nil {
		return err
	}
	rl := RateLimit{host: host, bucket: bucketTime, count: count}
	r.log.Info("ratelimits").Msgf("added rate limit for host: %s, bucket: %s, count: %d", host, bucket, count)
	r.list = append(r.list, rl)
	return nil
}

func (r *RateLimits) isRateLimited(path string) (bool, error) {
	var matchedRateLimit *RateLimit
	for _, limit := range r.list {
		matched, err := limit.match(path)
		if err != nil {
			return true, err
		}
		if matched {
			matchedRateLimit = &limit
			break
		}
	}

	if matchedRateLimit == nil {
		r.log.Debug("getter.ratelimits.isRateLimited").Msgf("No rate limit for %s", path)
		return false, nil
	}

	ctx := context.TODO()
	key := matchedRateLimit.getCacheKey()

	if r.cache == nil {
		r.log.Error("getter.ratelimits.isRateLimited").Msgf("Cache is not setup. Use Getter.SetCache to link redis to getter")
		return false, nil
	}

	val, err := r.cache.Get(ctx, key)
	if err != nil {
		r.log.LogError("isRateLimited.Get", err)
		return true, ErrRateLimitingDown
	}

	var limit int64
	if val != "" {
		limit, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			r.log.LogError("isRateLimited.ParseInt", err)
			return true, ErrRateLimitingInvalid
		}
	}

	if limit > matchedRateLimit.count {
		return true, nil
	}

	incrVal, err := r.cache.Incr(ctx, key)
	if err != nil {
		r.log.LogError("isRateLimited.Incr", err)
		return true, ErrRateLimitingDown
	}

	if incrVal > matchedRateLimit.count {
		return true, nil
	}
	// r.log.Info().Msgf("INCR %d %s", incrVal, key)

	_, err = r.cache.Expire(ctx, key, 4*matchedRateLimit.bucket)
	if err != nil {
		r.log.LogError("isRateLimited.Expire", err)
		return true, ErrRateLimitingDown
	}

	return false, nil
}
