// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nopaxos

import (
	"container/list"
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	nopaxos "github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/util"
	"math"
	"sync"
	"time"
)

// NewClient returns a new NOPaxos client
func NewClient(config cluster.Cluster, sequencerConfig *SequencerConfig) (*Client, error) {
	cluster := nopaxos.NewCluster(config)
	return newClient(cluster, sequencerConfig)
}

// newClient returns a new NOPaxos client
func newClient(cluster nopaxos.Cluster, config *SequencerConfig) (*Client, error) {
	conns := list.New()
	for _, member := range cluster.Members() {
		stream, err := cluster.GetStream(member)
		if err != nil {
			return nil, err
		}
		conns.PushBack(stream)
	}
	quorum := int(math.Floor(float64(conns.Len())/2.0)) + 1
	client := &Client{
		cluster:  cluster,
		config:   config,
		conns:    conns,
		quorum:   quorum,
		commands: make(map[nopaxos.MessageID]*commandHandler),
		queries:  make(map[nopaxos.MessageID]*queryHandler),
		log:      util.NewNodeLogger(string(cluster.Member())),
	}
	client.connect()
	return client, nil
}

// Client is a service Client implementation for the NOPaxos consensus protocol
type Client struct {
	node.Client
	cluster   nopaxos.Cluster
	config    *SequencerConfig
	conns     *list.List
	quorum    int
	commandID nopaxos.MessageID
	queryID   nopaxos.MessageID
	commands  map[nopaxos.MessageID]*commandHandler
	queries   map[nopaxos.MessageID]*queryHandler
	mu        sync.RWMutex
	log       util.Logger
}

func (c *Client) connect() {
	conn := c.conns.Front()
	for conn != nil {
		stream := conn.Value.(nopaxos.ClientService_ClientStreamClient)
		go c.receive(stream)
		conn = conn.Next()
	}
}

func (c *Client) receive(stream nopaxos.ClientService_ClientStreamClient) {
	for {
		response, err := stream.Recv()
		if err != nil {
			return
		}

		switch r := response.Message.(type) {
		case *nopaxos.ClientMessage_CommandReply:
			if r.CommandReply.ViewID.SessionNum != c.config.SessionId {
				continue
			}
			c.mu.Lock()
			handler := c.commands[r.CommandReply.MessageNum]
			complete := false
			if handler != nil && handler.succeed(r.CommandReply.Value) {
				delete(c.commands, r.CommandReply.MessageNum)
				complete = true
			}
			c.mu.Unlock()
			if complete {
				handler.complete()
			}
		case *nopaxos.ClientMessage_QueryReply:
			if r.QueryReply.ViewID.SessionNum != c.config.SessionId {
				continue
			}
		}
	}
}

// Write sends a write operation to the cluster
func (c *Client) Write(ctx context.Context, in []byte, ch chan<- node.Output) error {
	handler := &commandHandler{quorum: c.quorum}
	c.mu.Lock()
	messageID := c.commandID + 1
	c.commandID = messageID
	c.commands[messageID] = handler
	c.mu.Unlock()

	request := &nopaxos.CommandRequest{
		SessionNum: c.config.SessionId,
		MessageNum: messageID,
		Timestamp:  time.Now(),
		Value:      in,
	}

	go c.write(ctx, request, ch, handler)
	return nil
}

// Read sends a read operation to the cluster
func (c *Client) Read(ctx context.Context, in []byte, ch chan<- node.Output) error {
	handler := &queryHandler{}
	c.mu.Lock()
	messageID := c.queryID + 1
	c.queryID = messageID
	c.queries[messageID] = handler
	c.mu.Unlock()

	request := &nopaxos.QueryRequest{
		SessionNum: c.config.SessionId,
		MessageNum: messageID,
		Timestamp:  time.Now(),
		Value:      in,
	}

	go c.read(ctx, request, ch, handler)
	return nil
}

func (c *Client) write(ctx context.Context, request *nopaxos.CommandRequest, ch chan<- node.Output, handler *commandHandler) {
	conn := c.conns.Front()
	for conn != nil {
		err := conn.Value.(nopaxos.ClientService_ClientStreamClient).Send(&nopaxos.ClientMessage{
			Message: &nopaxos.ClientMessage_Command{
				Command: request,
			},
		})
		if err != nil {
			handler.fail()
		}
		conn = conn.Next()
	}
}

func (c *Client) read(ctx context.Context, request *nopaxos.QueryRequest, ch chan<- node.Output, handler *queryHandler) {
	conn := c.conns.Front()
	for conn != nil {
		_ = conn.Value.(nopaxos.ClientService_ClientStreamClient).Send(&nopaxos.ClientMessage{
			Message: &nopaxos.ClientMessage_Query{
				Query: request,
			},
		})
		conn = conn.Next()
	}
}

// commandHandler is a quorum reply handler
type commandHandler struct {
	quorum    int
	succeeded int
	failed    int
	value     []byte
	ch        chan<- node.Output
}

func (r *commandHandler) succeed(value []byte) bool {
	if r.value == nil && len(value) > 0 {
		r.value = value
	}
	r.succeeded++
	return r.value != nil && r.succeeded >= r.quorum
}

func (r *commandHandler) fail() bool {
	r.failed++
	return r.failed >= r.quorum
}

func (r *commandHandler) complete() {
	r.ch <- node.Output{
		Value: r.value,
	}
}

// queryHandler is a query reply handler
type queryHandler struct {
	ch chan<- node.Output
}

func (r *queryHandler) succeed(value []byte) bool {
	if len(value) > 0 {
		r.ch <- node.Output{
			Value: value,
		}
		return true
	}
	return false
}

// Close closes the client
func (c *Client) Close() error {
	return nil
}
