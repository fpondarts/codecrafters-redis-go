package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/resp"
)

type Event struct {
	Conn    net.Conn
	Payload resp.RESPElement
	Result  chan error
}

type TCPServer struct {
	Listener   net.Listener
	Clients    map[net.Conn]struct{}
	ClientsMtx sync.Mutex
	EventQueue chan Event
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
		_, err := conn.Read(buf)
		if err != nil {
			return
		}

		payload, _, err := resp.ParseRESP(buf)
		if err != nil {
			fmt.Println("bad resp input")
			return
		}
		errChan := make(chan error, 1024)
		server.EventQueue <- Event{Conn: conn, Payload: payload, Result: errChan}

		if err := <-errChan; err != nil {
			fmt.Printf("Error handling ping")
			return
		}
	}
}

func (server *TCPServer) EventLoop() {
	for event := range server.EventQueue {
		err := HandleEvent(event)
		event.Result <- err
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

func HandlePing(conn net.Conn) error {
	_, err := conn.Write([]byte("+PONG\r\n"))
	return err
}

func HandleArray(conn net.Conn, array resp.RESPArray) error {
	if len(array.Elements) != 2 {
		return fmt.Errorf("cant handle command")
	}

	command, ok := array.Elements[0].(resp.RESPBulkString)

	if !ok || strings.ToLower(command.Value) != "echo" {
		return fmt.Errorf("cant handle command %s", command.Value)
	}

	toEcho, ok := array.Elements[1].(resp.RESPBulkString)

	if !ok {
		return fmt.Errorf("cant echo %v", array.Elements[1])
	}

	_, err := conn.Write(resp.EncodeBulkString(toEcho.Value))
	return err
}

func HandleEvent(ev Event) error {
	switch payload := ev.Payload.(type) {
	case resp.RESPBulkString:
		if payload.Value == "PING" {
			return HandlePing(ev.Conn)
		}
	case resp.RESPArray:

	}
	return nil
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
		EventQueue: make(chan Event, 1000),
	}, nil
}

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var (
	_ = net.Listen
	_ = os.Exit
	_ = resp.ParseRESP
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	server, err := NewTCPServer(ServerConfig{
		IP:   net.ParseIP("0.0.0.0"),
		Port: 6379,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = server.Start()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
