package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/redis"
)

type Event struct {
	Conn    net.Conn
	Command redis.Command
	Result  chan error
}

type TCPServer struct {
	Listener   net.Listener
	Clients    map[net.Conn]struct{}
	ClientsMtx sync.Mutex
	EventQueue chan Event
}

var Storage = redis.NewStorage()

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

		el, _, err := redis.ParseRESP(buf)
		if err != nil {
			log.Printf("parse error from %s: %v", conn.RemoteAddr(), err)
			return
		}
		cmd, err := redis.ParseCommand(el)
		if err != nil {
			log.Printf("command parse error from %s: %v", conn.RemoteAddr(), err)
			return
		}
		errChan := make(chan error, 1024)
		server.EventQueue <- Event{Conn: conn, Command: cmd, Result: errChan}

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

func HandleEcho(conn net.Conn, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("ECHO requires 1 argument, got %d", len(args))
	}
	log.Printf("sending ECHO %q to %s", args[0], conn.RemoteAddr())
	_, err := conn.Write(redis.EncodeBulkString(args[0]))
	if err != nil {
		log.Printf("error sending ECHO to %s: %v", conn.RemoteAddr(), err)
	}
	return err
}

func HandleSet(conn net.Conn, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("SET requires at least 2 arguments, got %d", len(args))
	}
	key, value := args[0], args[1]

	var expiration time.Time
	if len(args) > 2 {
		if len(args) != 4 {
			return fmt.Errorf("SET option requires a value")
		}
		n, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid expiration value %q", args[3])
		}
		switch strings.ToUpper(args[2]) {
		case "EX":
			expiration = time.Now().Add(time.Duration(n) * time.Second)
		case "PX":
			expiration = time.Now().Add(time.Duration(n) * time.Millisecond)
		default:
			return fmt.Errorf("unknown SET option %q", args[2])
		}
	}

	log.Printf("SET %q = %q expiration=%v (from %s)", key, value, expiration, conn.RemoteAddr())
	if err := Storage.Set(key, value, expiration); err != nil {
		log.Printf("SET error for %q from %s: %v", key, conn.RemoteAddr(), err)
		_, werr := conn.Write(redis.EncodeError(err.Error()))
		return werr
	}
	_, err := conn.Write(redis.EncodeSimpleString("OK"))
	if err != nil {
		log.Printf("error sending SET response to %s: %v", conn.RemoteAddr(), err)
	}
	return err
}

func HandleGet(conn net.Conn, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("GET requires 1 argument, got %d", len(args))
	}
	key := args[0]
	val, ok, err := Storage.Get(key)
	if err != nil {
		log.Printf("GET error for %q from %s: %v", key, conn.RemoteAddr(), err)
		_, werr := conn.Write(redis.EncodeError(err.Error()))
		return werr
	}
	if !ok {
		log.Printf("GET %q -> nil (from %s)", key, conn.RemoteAddr())
		_, err := conn.Write(redis.EncodeNullBulkString())
		return err
	}
	log.Printf("GET %q -> %q (from %s)", key, val, conn.RemoteAddr())
	_, err = conn.Write(redis.EncodeBulkString(val))
	if err != nil {
		log.Printf("error sending GET response to %s: %v", conn.RemoteAddr(), err)
	}
	return err
}

func HandleRPush(conn net.Conn, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("RPUSH requires at least 2 arguments, got %d", len(args))
	}
	key, vals := args[0], args[1:]
	log.Printf("RPUSH %q %v (from %s)", key, vals, conn.RemoteAddr())
	n, err := Storage.RPush(key, vals...)
	if err != nil {
		log.Printf("RPUSH error for %q from %s: %v", key, conn.RemoteAddr(), err)
		_, werr := conn.Write(redis.EncodeError(err.Error()))
		return werr
	}
	_, err = conn.Write(redis.EncodeInteger(int64(n)))
	if err != nil {
		log.Printf("error sending RPUSH response to %s: %v", conn.RemoteAddr(), err)
	}
	return err
}

func HandleEvent(ev Event) error {
	switch ev.Command.Name {
	case "PING":
		return HandlePing(ev.Conn)
	case "ECHO":
		return HandleEcho(ev.Conn, ev.Command.Args)
	case "SET":
		return HandleSet(ev.Conn, ev.Command.Args)
	case "GET":
		return HandleGet(ev.Conn, ev.Command.Args)
	case "RPUSH":
		return HandleRPush(ev.Conn, ev.Command.Args)
	default:
		log.Printf("unhandled command %q from %s", ev.Command.Name, ev.Conn.RemoteAddr())
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
