package main

import (
	"fmt"
	"log"
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

		payload, _, err := resp.ParseRESP(buf)
		if err != nil {
			log.Printf("parse error from %s: %v", conn.RemoteAddr(), err)
			return
		}
		errChan := make(chan error, 1024)
		server.EventQueue <- Event{Conn: conn, Payload: payload, Result: errChan}

		if err := <-errChan; err != nil {
			log.Printf("error handling event from %s: %v", conn.RemoteAddr(), err)
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
	log.Printf("sending PONG to %s", conn.RemoteAddr())
	_, err := conn.Write([]byte("+PONG\r\n"))
	if err != nil {
		log.Printf("error sending PONG to %s: %v", conn.RemoteAddr(), err)
	}
	return err
}

func HandleArray(conn net.Conn, array resp.RESPArray) error {
	command, ok := array.Elements[0].(resp.RESPBulkString)

	if !ok {
		return fmt.Errorf("cant handle command %s", command.Value)
	}

	lowCaseCommand := strings.ToLower(command.Value)

	if lowCaseCommand == "ping" {
		return HandlePing(conn)
	}

	if lowCaseCommand != "echo" {
		return fmt.Errorf("cant handle command %s", command.Value)
	}

	toEcho, ok := array.Elements[1].(resp.RESPBulkString)

	if !ok {
		return fmt.Errorf("cant echo %v", array.Elements[1])
	}

	log.Printf("sending ECHO %q to %s", toEcho.Value, conn.RemoteAddr())
	_, err := conn.Write(resp.EncodeBulkString(toEcho.Value))
	if err != nil {
		log.Printf("error sending ECHO to %s: %v", conn.RemoteAddr(), err)
	}
	return err
}

func HandleEvent(ev Event) error {
	switch payload := ev.Payload.(type) {
	case resp.RESPBulkString:
		if payload.Value == "PING" {
			return HandlePing(ev.Conn)
		}
		log.Printf("unhandled bulk string command %q from %s", payload.Value, ev.Conn.RemoteAddr())
	case resp.RESPArray:
		return HandleArray(ev.Conn, payload)
	default:
		log.Printf("unhandled payload type %T from %s", payload, ev.Conn.RemoteAddr())
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
	fmt.Println("Logs from your program will appear here!")
	server, err := NewTCPServer(ServerConfig{
		IP:   net.ParseIP("0.0.0.0"),
		Port: 6379,
	})
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	log.Printf("listening on %s", server.Listener.Addr())
	if err := server.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
