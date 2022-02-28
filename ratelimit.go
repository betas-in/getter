package getter

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// RateLimit ...
type RateLimit struct {
	host   string
	bucket time.Duration
	count  int64
}

func (r *RateLimit) getHostFromURL(path string) (string, error) {
	u, err := url.ParseRequestURI(path)
	if err != nil {
		return "", err
	}
	return u.Host, nil
}

func (r *RateLimit) match(path string) (bool, error) {
	host, err := r.getHostFromURL(path)
	if err != nil {
		return false, err
	}
	return strings.Contains(host, r.host), nil
}

func (r *RateLimit) getCacheKey() string {
	return fmt.Sprintf("%s%s%s%s%d", keyRateLimit, separator, r.host, separator, r.getBucketTimestamp())
}

func (r *RateLimit) getBucketTimestamp() int64 {
	ts := time.Now()
	gap := (ts.Unix() % int64(r.bucket/time.Second))
	ts = ts.Add(time.Duration(-1*gap) * time.Second)
	return ts.Unix()
}
