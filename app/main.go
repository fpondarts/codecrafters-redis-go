package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/redis"
)

type Event struct {
	Conn   net.Conn
	Data   []byte
	Result chan error
}

type TCPServer struct {
	Listener   net.Listener
	Clients    map[net.Conn]struct{}
	ClientsMtx sync.Mutex
	EventQueue chan Event
	HandleFunc func([]byte) ([]byte, error)
}

func (server *TCPServer) HandleConnection(conn net.Conn) {
	log.Printf("new connection from %s", conn.RemoteAddr())
	server.ClientsMtx.Lock()
	server.Clients[conn] = struct{}{}
	server.ClientsMtx.Unlock()

	defer func() {
		log.Printf("connection closed: %s", conn.RemoteAddr())
		conn.Close()
		server.ClientsMtx.Lock()
		delete(server.Clients, conn)
		server.ClientsMtx.Unlock()
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
		server.EventQueue <- Event{Conn: conn, Data: data, Result: errChan}
		if err := <-errChan; err != nil {
			log.Printf("write error for %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

func (server *TCPServer) EventLoop() {
	for event := range server.EventQueue {
		resp, err := server.HandleFunc(event.Data)
		if err != nil {
			log.Printf("internal error handling event: %v", err)
			event.Result <- err
			continue
		}
		_, werr := event.Conn.Write(resp)
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

func NewTCPServer(config ServerConfig, handleFunc func([]byte) ([]byte, error)) (*TCPServer, error) {
	addr := &net.TCPAddr{
		IP:   config.IP,
		Port: config.Port,
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to start server")
	}

	return &TCPServer{
		Listener:   listener,
		Clients:    make(map[net.Conn]struct{}),
		EventQueue: make(chan Event, 1000),
		HandleFunc: handleFunc,
	}, nil
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var (
	_ = net.Listen
	_ = os.Exit
)

func main() {
	fmt.Println("Logs from your program will appear here!")
	r := redis.NewRedis()
	server, err := NewTCPServer(ServerConfig{
		IP:   net.ParseIP("0.0.0.0"),
		Port: 6379,
	}, r.Handle)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	log.Printf("listening on %s", server.Listener.Addr())
	if err := server.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
