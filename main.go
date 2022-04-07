package main

import (
	"errors"
	"sync"
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
	OnNewRemoteConnection(ipAddress int32, conn IConnection)
	Shutdown()
}

type ConnectionCache struct {
	store           sync.Map
	buildConnection func(ipAddress int32) IConnection
}

var _ IConnectionCache = (*ConnectionCache)(nil)

func NewConnectionCache() *ConnectionCache {
	return &ConnectionCache{
		buildConnection: func(ipAddress int32) IConnection {
			return &Connection{}
		},
	}
}

type safeConnection struct {
	conn        IConnection
	established bool
	mu          sync.Mutex

	establisedIn  chan IConnection
	establisedOut chan IConnection
}

func createOrLoadSafeConn(cache *ConnectionCache, ipAddress int32) (*safeConnection, bool, error) {
	val, existed := cache.store.LoadOrStore(ipAddress, &safeConnection{
		establisedIn:  make(chan IConnection, 1),
		establisedOut: make(chan IConnection, 1),
	})

	safeConn, ok := val.(*safeConnection)
	if !ok {
		return nil, false, errors.New("invalid map type") // TODO: add proper error
	}

	if !existed { // run just once for specific ip

		// NOTE: in the real life I suggest this function to be runned for the whole app lifecycle
		// since it may be used for reconnection as well
		go func(safeConn *safeConnection) {
			establisedConn := <-safeConn.establisedIn
			safeConn.conn = establisedConn
			safeConn.established = true // it's better to use atomics here or wrap by additional mutex
			safeConn.establisedOut <- establisedConn
			// here we may cancel context passed to conn.Open(ctx)
		}(safeConn)
	}

	return safeConn, existed, nil
}

func (cache *ConnectionCache) GetConnection(ipAddress int32) (IConnection, error) {
	safeConn, existed, err := createOrLoadSafeConn(cache, ipAddress)
	if err != nil {
		return nil, err
	}

	safeConn.mu.Lock() // lock for every future call for the same ip
	defer safeConn.mu.Unlock()

	if !existed {
		defer close(safeConn.establisedOut)

		go func(safeConn *safeConnection) {
			conn := cache.buildConnection(ipAddress)
			conn.Open() // sync operation
			safeConn.establisedIn <- conn
		}(safeConn)

		return <-safeConn.establisedOut, nil
	}

	return safeConn.conn, nil
}

func (cache *ConnectionCache) OnNewRemoteConnection(ipAddress int32, conn IConnection) {
	safeConn, _, err := createOrLoadSafeConn(cache, ipAddress)
	if err != nil {
		return // TODO: handle this case propertly
	}
	if safeConn.established {
		conn.Close()
	} else {
		safeConn.establisedIn <- conn
	}
}

func (cache *ConnectionCache) Shutdown() {
	// TODO: add flag meaning shutdown in progress
	// and check this flag in GetConnection / OnNewRemoteConnection
	// rejecting new connection

	var wg sync.WaitGroup
	cache.store.Range(func(_, val any) bool {
		wg.Add(1)

		safeConn, ok := val.(*safeConnection)
		if !ok {
			return true // continue
		}

		go func(safeConn *safeConnection) {
			defer wg.Done()
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
