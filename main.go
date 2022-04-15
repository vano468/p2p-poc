package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type IConnection interface {
	Open()
	Close()
}

type Connection struct{}

var _ IConnection = (*Connection)(nil)

func (c *Connection) Open()  { time.Sleep(3 * time.Second) }
func (c *Connection) Close() {}

type IConnectionCache interface {
	GetConnection(ipAddress int32) (IConnection, error)
	OnNewRemoteConnection(ipAddress int32, conn IConnection) error
	Shutdown()
}

type ConnectionCache struct {
	store           sync.Map
	buildConnection func(ipAddress int32) IConnection
	inShutdown      *int32
}

var _ IConnectionCache = (*ConnectionCache)(nil)

func NewConnectionCache() *ConnectionCache {
	inShutdown := int32(0)
	return &ConnectionCache{
		inShutdown: &inShutdown,
		buildConnection: func(ipAddress int32) IConnection {
			return &Connection{}
		},
	}
}

type safeConnection struct {
	ipAddress   int32
	conn        IConnection
	established *int32
	mu          sync.Mutex
	ch          chan IConnection
}

func (safeConn *safeConnection) EstablishOrClose(conn IConnection) {
	if atomic.CompareAndSwapInt32(safeConn.established, 0, 1) {
		safeConn.ch <- conn
	} else {
		conn.Close()
	}
}

func createOrLoadSafeConn(
	cache *ConnectionCache, ipAddress int32, establised *int32, conn IConnection,
) (*safeConnection, bool, error) {
	val, existed := cache.store.LoadOrStore(ipAddress, &safeConnection{
		ipAddress:   ipAddress,
		ch:          make(chan IConnection, 1),
		established: establised,
		conn:        conn,
	})

	safeConn, ok := val.(*safeConnection)
	if !ok {
		return nil, false, fmt.Errorf(`*safeConnection expected, but got: %T`, safeConn)
	}

	return safeConn, existed, nil
}

func (cache *ConnectionCache) GetConnection(ipAddress int32) (IConnection, error) {
	if atomic.LoadInt32(cache.inShutdown) == 1 {
		return nil, nil
	}

	establised := int32(0)
	safeConn, _, err := createOrLoadSafeConn(cache, ipAddress, &establised, nil)
	if err != nil {
		return nil, err
	}

	safeConn.mu.Lock() // lock for every future call for the same ip
	defer safeConn.mu.Unlock()

	if atomic.LoadInt32(safeConn.established) == 1 {
		return safeConn.conn, nil
	}

	go func(safeConn *safeConnection) {
		conn := cache.buildConnection(ipAddress)
		conn.Open() // sync operation
		safeConn.EstablishOrClose(conn)
	}(safeConn)

	safeConn.conn = <-safeConn.ch
	return safeConn.conn, nil
}

func (cache *ConnectionCache) OnNewRemoteConnection(ipAddress int32, conn IConnection) error {
	if atomic.LoadInt32(cache.inShutdown) == 1 {
		conn.Close()
		return nil
	}

	establised := int32(1)
	safeConn, existed, err := createOrLoadSafeConn(cache, ipAddress, &establised, conn)
	if err != nil {
		return err
	}

	if existed && atomic.LoadInt32(safeConn.established) == 1 {
		conn.Close()
	} else if existed { // existed but not established
		safeConn.EstablishOrClose(conn)
	}

	return nil
}

func (cache *ConnectionCache) Shutdown() {
	if !atomic.CompareAndSwapInt32(cache.inShutdown, 0, 1) {
		return // already started
	}

	var wg sync.WaitGroup
	cache.store.Range(func(_, val any) bool {
		wg.Add(1)

		safeConn, ok := val.(*safeConnection)
		if !ok {
			return true // continue
		}

		go func(safeConn *safeConnection) {
			defer wg.Done()

			safeConn.mu.Lock()
			defer safeConn.mu.Unlock()

			safeConn.conn.Close()
		}(safeConn)

		return true
	})
	wg.Wait()
}

func main() {
	remoteNodesIPs := []int32{
		1, 2, 3,
		1, 2, 3,
		1, 2, 3,
	}
	cache := NewConnectionCache()

	var wg sync.WaitGroup
	for _, ip := range remoteNodesIPs {
		wg.Add(1)

		go func(ip int32) {
			defer wg.Done()

			_, err := cache.GetConnection(ip)
			if err != nil {
				panic(err)
			}
		}(ip)
	}

	time.Sleep(time.Second)
	go cache.OnNewRemoteConnection(1, &Connection{})
	go cache.OnNewRemoteConnection(2, &Connection{})
	go cache.OnNewRemoteConnection(3, &Connection{})
	wg.Wait()

	cache.Shutdown()
}
