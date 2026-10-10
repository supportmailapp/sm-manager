package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"time"

	"sm-manager/internal/config"
	"sm-manager/internal/protocol"
)

// Server owns the Unix socket listener and drives the hub.
type Server struct {
	cfg *config.Config
	hub *Hub
	ln  net.Listener
}

func New(cfg *config.Config) *Server {
	return &Server{cfg: cfg, hub: NewHub(cfg)}
}

// Hub exposes the server's Hub so main can wire a Deployer (the supervisor)
// into it before Serve starts accepting connections.
func (s *Server) Hub() *Hub {
	return s.hub
}

// Listen binds the Unix socket, removing a stale socket file first.
func (s *Server) Listen() error {
	if err := os.Remove(s.cfg.SocketPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	ln, err := net.Listen("unix", s.cfg.SocketPath)
	if err != nil {
		return err
	}
	s.ln = ln
	log.Printf("listening on unix socket %s", s.cfg.SocketPath)
	return nil
}

// Serve accepts connections until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	go s.reapLoop(ctx)

	go func() {
		<-ctx.Done()
		_ = s.ln.Close()
	}()

	for {
		nc, err := s.ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("accept error: %v", err)
			continue
		}
		go s.handleConn(nc)
	}
}

func (s *Server) handleConn(nc net.Conn) {
	c := s.hub.newConn(nc)
	log.Printf("conn %d opened", c.id)
	defer func() {
		s.hub.removeConn(c)
		_ = nc.Close()
		log.Printf("conn %d closed", c.id)
	}()

	r := bufio.NewReader(nc)
	for {
		raw, err := protocol.ReadFrame(r)
		if err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				log.Printf("conn %d: read error: %v", c.id, err)
			}
			return
		}
		s.hub.handleFrame(c, raw)
	}
}

func (s *Server) reapLoop(ctx context.Context) {
	interval := time.Duration(s.cfg.HeartbeatTimeoutSecs) * time.Second / 2
	if interval < time.Second {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.hub.reapDead()
		}
	}
}

// Close removes the socket file.
func (s *Server) Close() {
	if s.ln != nil {
		_ = s.ln.Close()
	}
	_ = os.Remove(s.cfg.SocketPath)
}
