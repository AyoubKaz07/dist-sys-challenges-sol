package main

import (
    "log"
		"encoding/json"
		"time"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
		//"golang.org/x/exp/maps"
)

import "sync"

type Server struct {
	Node     *maelstrom.Node
	Topology map[string][]string
	Values   sync.Map
}

func NewServer(node *maelstrom.Node) *Server {
	s := &Server{
		Node: node,
		Values: sync.Map{},
	}

	node.Handle("broadcast", s.BroadcastHandler)
	node.Handle("read", s.ReadHandler)
	node.Handle("topology", s.TopologyHandler)

	return s
}


func (s *Server) Run() error {
	return s.Node.Run()
}

func (s *Server) BroadcastHandler(msg maelstrom.Message) error {
	var body struct {
		Message int `json:"message"`
		MessageID int `json:"msg_id"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	
	s.Node.Reply(msg, map[string]any {
		"type": "broadcast_ok",
	})

	if _, ok := s.Values.Load(body.Message); ok {
		return nil
	}
	s.Values.Store(body.Message, true)
	// Broadcast to all nodes in the topology
	for _, node := range s.Topology[s.Node.ID()] {
		if node == s.Node.ID() {
			continue
		}
		// Skip the source node
		if (msg.Src == node) {
			continue
		}
		var ack_mu sync.Mutex
		var acknowledge bool = false
		for !acknowledge {
			s.Node.RPC(node, map[string]any {
				"type": "broadcast",
				"message": body.Message,
			},
			func(reply maelstrom.Message) error {
				ack_mu.Lock()
				defer ack_mu.Unlock()
				acknowledge = true
				return nil
			})
			// add a delay to prevent busy waiting
			time.Sleep(500 * time.Millisecond)
		}
	}
	if body.MessageID == 0 {
		return nil
	}

	return s.Node.Reply(msg, struct {
		Type string `json:"type"`
	}{
		Type: "broadcast_ok",
	})
}

func (s* Server) TopologyHandler(msg maelstrom.Message) error {
	var body struct {
		Topology map[string][]string `json:"topology"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.Topology = body.Topology
	
	return s.Node.Reply(msg, struct {
		Type string `json:"type"`
	}{
		Type: "topology_ok",
	})
} 

func (s *Server) ReadHandler(msg maelstrom.Message) error {	
	keys := make([]int, 0)
	length := 0
	s.Values.Range(func(key, _ interface{}) bool {
		keys = append(keys, key.(int))
		length++
		return true
	})

	return s.Node.Reply(msg, struct {
		Type     string `json:"type"`
		Messages []int  `json:"messages"`
	}{
		Type:     "read_ok",
		Messages: keys,
	})
}

func main() {
	server := NewServer(maelstrom.NewNode())
	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
}