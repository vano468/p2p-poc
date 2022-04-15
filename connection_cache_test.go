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

func newConnectionCacheWithMockedConn(conn IConnection) *ConnectionCache {
	inShutdown := int32(0)
	return &ConnectionCache{
		inShutdown: &inShutdown,
		buildConnection: func(ipAddress int32) IConnection {
			return conn
		},
	}
}

func TestCache_GetConnection(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	conn := NewMockIConnection(ctl)
	conn.EXPECT().Open().DoAndReturn(func() {
		time.Sleep(connectionLatency)
	}).Times(3)

	cache := newConnectionCacheWithMockedConn(conn)

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

func TestCache_OnNewRemoteConnection(t *testing.T) {
	t.Run("replaces non-establised conn", func(t *testing.T) {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		originalConn := NewMockIConnection(ctl)
		originalConn.EXPECT().Open().DoAndReturn(func() {
			time.Sleep(connectionLatency * 100)
		}).AnyTimes()

		cache := newConnectionCacheWithMockedConn(originalConn)

		returnedConnection := make(chan IConnection)
		go func() {
			conn, err := cache.GetConnection(1)
			require.NoError(t, err)
			require.NotNil(t, conn)
			returnedConnection <- conn
		}()
		time.Sleep(connectionLatency / 2)

		remoteConnection := &Connection{}
		cache.OnNewRemoteConnection(1, remoteConnection)

		assert.Equal(t, remoteConnection, <-returnedConnection)

		actualConnection, err := cache.GetConnection(1)
		require.NoError(t, err)
		assert.Equal(t, remoteConnection, actualConnection)
	})

	t.Run("returns already establised conn", func(t *testing.T) {
		ctl := gomock.NewController(t)
		defer ctl.Finish()

		originalConn := NewMockIConnection(ctl)
		originalConn.EXPECT().Open().DoAndReturn(func() {
			time.Sleep(connectionLatency / 2)
		}).AnyTimes()

		cache := newConnectionCacheWithMockedConn(originalConn)

		go cache.GetConnection(1)
		time.Sleep(connectionLatency)

		cache.OnNewRemoteConnection(1, &Connection{})
		actualConnection, err := cache.GetConnection(1)

		require.NoError(t, err)
		assert.Equal(t, originalConn, actualConnection)
	})
}

func TestCache_Shutdown(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	conn := NewMockIConnection(ctl)
	conn.EXPECT().Open().Times(3)
	conn.EXPECT().Close().Times(3)

	cache := newConnectionCacheWithMockedConn(conn)

	var wg sync.WaitGroup
	for _, ip := range []int32{1, 2, 3} {
		wg.Add(1)

		go func(ip int32) {
			defer wg.Done()

			_, err := cache.GetConnection(ip)
			require.NoError(t, err)
		}(ip)
	}
	wg.Wait()

	for i := 0; i < 3; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			cache.Shutdown()
		}()
	}
	wg.Wait()
}
