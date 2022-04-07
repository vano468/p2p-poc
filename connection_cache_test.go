package main

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const connectionLatency = 200 * time.Millisecond

func TestCache_GetConnection(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	conn := NewMockIConnection(ctl)
	conn.EXPECT().Open().DoAndReturn(func() {
		time.Sleep(connectionLatency)
	}).Times(3)

	cache := ConnectionCache{
		buildConnection: func(ipAddress int32) IConnection {
			return conn
		},
	}

	initialTime := time.Now()
	var wg sync.WaitGroup

	for _, ip := range []int32{
		1, 2, 3,
		1, 2, 3,
		1, 2, 3,
	} {
		wg.Add(1)

		go func(ip int32) {
			defer wg.Done()
			_, err := cache.GetConnection(ip)
			require.NoError(t, err)
		}(ip)
	}
	wg.Wait()

	latencyDelta := 5 * time.Millisecond
	assert.LessOrEqual(t, time.Since(initialTime), connectionLatency+latencyDelta)
	assert.GreaterOrEqual(t, time.Since(initialTime), connectionLatency-latencyDelta)
}
