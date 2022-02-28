package getter

import (
	"os"
	"testing"
	"time"

	"github.com/betas-in/logger"
	"github.com/betas-in/rediscache"
	"github.com/betas-in/utils"
)

func TestGet(t *testing.T) {
	log := logger.NewLogger(1, true)

	url := "http://feeds.arstechnica.com/arstechnica/index"

	g := NewGetter(log)

	err := g.AddRateLimit("arstechnica.com", "1h", 1)
	utils.Test().Nil(t, err)

	response := g.FetchResponse(Request{
		Path: url,
	})
	utils.Test().Nil(t, response.Error)
	utils.Test().Equals(t, 200, response.Code)
	utils.Test().Contains(t, string(response.Data), "https://arstechnica.com/")

	response = g.FetchResponse(Request{
		Path: "http://gotrixsterinmypajamas.com",
	})
	utils.Test().Equals(t, 0, response.Code)
	utils.Test().Contains(t, response.Error.Error(), "dial tcp")
}

func TestZipped(t *testing.T) {
	log := logger.NewLogger(1, true)

	url := "https://github.com/gojekfarm/async-worker/archive/refs/heads/master.zip"

	g := NewGetter(log)
	response := g.FetchResponse(Request{
		Path: url,
	})
	utils.Test().Nil(t, response.Error)
	utils.Test().Equals(t, 200, response.Code)
	utils.Test().Equals(t, "zip", response.ContentType)
	os.Remove(response.DataPath)
}

func TestRateLimit(t *testing.T) {
	r := RateLimit{host: "amfiindia.com", bucket: time.Duration(5 * time.Second), count: 1}

	url1 := "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=04-Sep-2007"
	url2 := "https://www1.nseindia.com/content/historical/EQUITIES/2021/OCT/cm14OCT2021bhav.csv.zip"
	url3 := "https://republika.co.id/berita/r5fa6n370/jerman-kembangkan-vaksin-baru-untuk-hadapi-varian-virus-corona"

	u, err := r.getHostFromURL(url1)
	utils.Test().Nil(t, err)
	utils.Test().Equals(t, "portal.amfiindia.com", u)

	matched, err := r.match(url1)
	utils.Test().Nil(t, err)
	utils.Test().Equals(t, true, matched)

	u, err = r.getHostFromURL(url2)
	utils.Test().Nil(t, err)
	utils.Test().Equals(t, "www1.nseindia.com", u)

	matched, err = r.match(url2)
	utils.Test().Nil(t, err)
	utils.Test().Equals(t, false, matched)

	u, err = r.getHostFromURL(url3)
	utils.Test().Nil(t, err)
	utils.Test().Equals(t, "republika.co.id", u)

	matched, err = r.match(url3)
	utils.Test().Nil(t, err)
	utils.Test().Equals(t, false, matched)

	ts := r.getBucketTimestamp()
	utils.Test().Equals(t, true, ts <= time.Now().Unix())
}

func TestRateLimits(t *testing.T) {
	// Configuration
	log := logger.NewLogger(1, true)
	redisConf := rediscache.Config{
		Host:     "127.0.0.1",
		Port:     9876,
		Password: "596a96cc7bf9108cd896f33c44aedc8a",
	}

	c, err := rediscache.NewCache(&redisConf, log)
	utils.Test().Nil(t, err)

	url1 := "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=04-Sep-2007"
	url2 := "https://www1.nseindia.com/content/historical/EQUITIES/2021/OCT/cm14OCT2021bhav.csv.zip"
	// url3 := "https://republika.co.id/berita/r5fa6n370/jerman-kembangkan-vaksin-baru-untuk-hadapi-varian-virus-corona"

	r := RateLimits{log: log, cache: c, list: []RateLimit{}}

	iterations := 5
	maxCountURL1 := int64(2)
	maxCountURL2 := int64(10000)
	bucketSize := "1s"
	pollDuration := 5 * time.Millisecond

	err = r.addRateLimit("amfiindia.com", bucketSize, maxCountURL1)
	utils.Test().Nil(t, err)
	err = r.addRateLimit("nseindia.com", bucketSize, maxCountURL2)
	utils.Test().Nil(t, err)

	count := 0
	ticker := time.NewTicker(pollDuration)

	for range ticker.C {
		url1RL, err := r.isRateLimited(url1)
		utils.Test().Nil(t, err)
		if int64(count) < maxCountURL1 {
			utils.Test().Equals(t, false, url1RL)
		} else {
			utils.Test().Equals(t, true, url1RL)
		}

		url2RL, err := r.isRateLimited(url2)
		utils.Test().Nil(t, err)
		if int64(count) < maxCountURL2 {
			utils.Test().Equals(t, false, url2RL)
		} else {
			utils.Test().Equals(t, true, url2RL)
		}

		if count > iterations {
			break
		}
		count++
	}
}
