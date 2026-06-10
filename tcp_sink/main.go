package main

import (
	"net"
	"os"
)

// go build -o f5keeper main.go
// ./f5keeper   # listens on :9092

func main() {
	ln, err := net.Listen("tcp", ":9092")
	if err != nil {
		os.Exit(1)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		// SO_LINGER=0: kernel sends RST on close, no TIME_WAIT, FD freed instantly
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
		conn.Close()
	}
}
