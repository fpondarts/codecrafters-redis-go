package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/codecrafters-io/redis-starter-go/internal/redis"
)

type Event struct {
	ConnID uint64
	Conn   net.Conn
	Data   []byte
	Result chan error
}

type TCPServer struct {
	Listener    net.Listener
	Clients     map[net.Conn]struct{}
	ClientsMtx  sync.Mutex
	EventQueue  chan Event
	HandleFunc  func(connID uint64, buf []byte) (redis.Response, error)
	DisconnFunc func(connID uint64)
	connCounter atomic.Uint64
}

func (server *TCPServer) HandleConnection(conn net.Conn) {
	connID := server.connCounter.Add(1)
	log.Printf("new connection from %s (id=%d)", conn.RemoteAddr(), connID)
	server.ClientsMtx.Lock()
	server.Clients[conn] = struct{}{}
	server.ClientsMtx.Unlock()

	defer func() {
		log.Printf("connection closed: %s (id=%d)", conn.RemoteAddr(), connID)
		conn.Close()
		server.ClientsMtx.Lock()
		delete(server.Clients, conn)
		server.ClientsMtx.Unlock()
		server.DisconnFunc(connID)
	}()

	buf := make([]byte, 512)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		log.Printf("received %d bytes from %s: %q", n, conn.RemoteAddr(), buf[:n])
		data := make([]byte, n)
		copy(data, buf[:n])
		errChan := make(chan error, 1)
		server.EventQueue <- Event{ConnID: connID, Conn: conn, Data: data, Result: errChan}
		if err := <-errChan; err != nil {
			log.Printf("write error for %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

func (server *TCPServer) EventLoop() {
	for event := range server.EventQueue {
		resp, err := server.HandleFunc(event.ConnID, event.Data)
		if err != nil {
			log.Printf("internal error handling event: %v", err)
			event.Result <- err
			continue
		}
		if resp.Pending != nil {
			go func(e Event, pending <-chan []byte) {
				data := <-pending
				_, werr := e.Conn.Write(data)
				if werr != nil {
					log.Printf("write error to %s: %v", e.Conn.RemoteAddr(), werr)
				}
				e.Result <- werr
			}(event, resp.Pending)
			continue
		}
		_, werr := event.Conn.Write(resp.Data)
		if werr != nil {
			log.Printf("write error to %s: %v", event.Conn.RemoteAddr(), werr)
		}
		event.Result <- werr
	}
}

func (server *TCPServer) Start() error {
	go server.EventLoop()

	for {
		conn, err := server.Listener.Accept()
		if err != nil {
			return err
		}

		go server.HandleConnection(conn)
	}
}

type ServerConfig struct {
	IP   net.IP
	Port int
}

func NewTCPServer(
	config ServerConfig,
	handleFunc func(connID uint64, buf []byte) (redis.Response, error),
	disconnFunc func(connID uint64),
) (*TCPServer, error) {
	addr := &net.TCPAddr{
		IP:   config.IP,
		Port: config.Port,
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to start server")
	}

	return &TCPServer{
		Listener:    listener,
		Clients:     make(map[net.Conn]struct{}),
		EventQueue:  make(chan Event, 1000),
		HandleFunc:  handleFunc,
		DisconnFunc: disconnFunc,
	}, nil
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var (
	_ = net.Listen
	_ = os.Exit
)

func main() {
	port := 6379

	if i := slices.Index(os.Args, "--port"); i != -1 {
		parsedPort, err := strconv.Atoi(os.Args[i+1])
		if err != nil {
			log.Fatalf("bad --port argument")
		}

		port = parsedPort
	}

	var masterNode *redis.MasterNode = nil

	if i := slices.Index(os.Args, "--replicaof"); i != -1 {
		replicaString := os.Args[i+1]
		parts := strings.SplitN(replicaString, " ", 2)
		host, port := parts[0], parts[1]

		parsedIP, err := net.LookupIP(host)
		if err != nil {
			log.Fatalf("bad --replicaof HOST")
		}

		parsedPort, err := strconv.Atoi(port)
		if err != nil {
			log.Fatalf("bad --replicaof PORT")
		}

		masterNode = &redis.MasterNode{IP: parsedIP[0], Port: parsedPort}
	}
	redisConfig := redis.RedisConfig{Master: masterNode}
	fmt.Println("Logs from your program will appear here!")
	r := redis.NewRedis(redisConfig)

	if r == nil {
		log.Fatalf("Failed to init redis")
	}
	server, err := NewTCPServer(
		ServerConfig{
			IP:   net.ParseIP("0.0.0.0"),
			Port: port,
		},
		r.Handle,
		r.OnDisconnect,
	)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	log.Printf("listening on %s", server.Listener.Addr())
	if err := server.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
