package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

type PingEvent struct {
	Conn   net.Conn
	Amount int
	Result error
}

type TCPServer struct {
	Listener   net.Listener
	Clients    map[net.Conn]struct{}
	ClientsMtx sync.Mutex
	EventQueue chan PingEvent
}

func (server *TCPServer) HandleConnection(conn net.Conn) {
	server.ClientsMtx.Lock()
	server.Clients[conn] = struct{}{}
	server.ClientsMtx.Unlock()

	defer func() {
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

		data := string(buf[:n])

		pingCount := strings.Count(data, "PING")
		server.EventQueue <- PingEvent{Conn: conn, Amount: pingCount, Result: nil}
	}
}

func (server *TCPServer) EventLoop() {
	for event := range server.EventQueue {
		for range PingEvent.Amount {
			event.Conn.Write([]byte("+PONG\r\n"))
		}
	}
}

type ServerConfig struct {
	IP   net.IP
	Port int
}

func NewTCPServer(config ServerConfig) (*TCPServer, error) {
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
		EventQueue: make(chan PingEvent, 1000),
	}, nil
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var (
	_ = net.Listen
	_ = os.Exit
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	server, err := NewTCPServer(ServerConfig{
		IP:   net.ParseIP("0.0.0.0"),
		Port: 6379,
	})

	l := server.Listener

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go server.HandleConnection(conn)
	}
}
