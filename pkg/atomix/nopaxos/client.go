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
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/util"
	"math"
	"sort"
	"sync"
	"time"
)

const queueSize = 1000

// NewClient returns a new NOPaxos client
func NewClient(config cluster.Cluster, sequencerConfig *SequencerConfig) (*Client, error) {
	return newClient(NewCluster(config), sequencerConfig)
}

// newClient returns a new NOPaxos client
func newClient(cluster *Cluster, config *SequencerConfig) (*Client, error) {
	quorum := int(math.Floor(float64(len(cluster.Members()))/2.0)) + 1
	acks := make(map[protocol.MemberID]protocol.LogSlotID)
	for _, member := range cluster.Members() {
		acks[protocol.MemberID(member)] = 0
	}
	client := &Client{
		logger:     util.NewNodeLogger("sequencer"),
		cluster:    cluster,
		config:     config,
		writeChans: list.New(),
		readChans:  list.New(),
		quorum:     quorum,
		acks:       acks,
		commands:   make(map[protocol.MessageID]*commandHandler),
		queries:    make(map[protocol.MessageID]*queryHandler),
		writes:     make(chan *requestContext, queueSize),
		reads:      make(chan *requestContext, queueSize),
		log:        util.NewNodeLogger(string(cluster.Member())),
	}
	if err := client.connect(cluster); err != nil {
		return nil, err
	}
	return client, nil
}

// Client is a service Client implementation for the NOPaxos consensus protocol
type Client struct {
	node.Client
	logger     util.Logger
	cluster    *Cluster
	config     *SequencerConfig
	writeChans *list.List
	readChans  *list.List
	quorum     int
	commandID  protocol.MessageID
	queryID    protocol.MessageID
	acks       map[protocol.MemberID]protocol.LogSlotID
	commands   map[protocol.MessageID]*commandHandler
	queries    map[protocol.MessageID]*queryHandler
	writes     chan *requestContext
	reads      chan *requestContext
	mu         sync.RWMutex
	log        util.Logger
}

func (c *Client) connect(cluster *Cluster) error {
	for _, member := range cluster.Members() {
		stream, err := cluster.GetStream(member)
		if err != nil {
			return err
		}
		go c.receive(member, stream)
		writeCh := make(chan *protocol.CommandRequest)
		c.writeChans.PushBack(writeCh)
		go c.sendWrites(member, stream, writeCh)
		readCh := make(chan *protocol.QueryRequest)
		c.readChans.PushBack(readCh)
		go c.sendReads(member, stream, readCh)
	}
	go c.processWrites()
	go c.processReads()
	return nil
}

func (c *Client) receive(member MemberID, stream protocol.ClientService_ClientStreamClient) {
	for {
		response, err := stream.Recv()
		if err != nil {
			return
		}

		switch r := response.Message.(type) {
		case *protocol.ClientMessage_CommandReply:
			if r.CommandReply.ViewID.SessionNum != c.config.SessionId {
				continue
			}
			c.logger.ReceiveFrom("CommandReply", r.CommandReply, member)

			c.mu.Lock()

			// Record the slot ack
			c.acks[r.CommandReply.Sender] = r.CommandReply.SlotNum

			// Compute the new commit slot
			slots := make([]protocol.LogSlotID, 0, len(c.acks))
			for _, slotNum := range c.acks {
				slots = append(slots, slotNum)
			}
			sort.Slice(slots, func(i, j int) bool {
				return slots[i] < slots[j]
			})
			commitSlotNum := slots[c.quorum-1]

			// Get the command reply handler
			handler := c.commands[r.CommandReply.MessageNum]
			if handler != nil {
				// If a reply value was returned, set the value
				if r.CommandReply.Value != nil {
					handler.receive(r.CommandReply.SlotNum, r.CommandReply.Value)
				}
				handler.commit(commitSlotNum)
			}
			c.mu.Unlock()
		case *protocol.ClientMessage_CommandClose:
			if r.CommandClose.ViewID.SessionNum != c.config.SessionId {
				continue
			}
			c.logger.ReceiveFrom("CommandClose", r.CommandClose, member)
			c.mu.Lock()
			handler := c.commands[r.CommandClose.MessageNum]
			delete(c.commands, r.CommandClose.MessageNum)
			c.mu.Unlock()
			if handler != nil {
				handler.complete()
			}
		case *protocol.ClientMessage_QueryReply:
			if r.QueryReply.ViewID.SessionNum != c.config.SessionId {
				continue
			}
			c.logger.ReceiveFrom("QueryReply", r.QueryReply, member)
			if r.QueryReply.Value != nil {
				c.mu.RLock()
				handler := c.queries[r.QueryReply.MessageNum]
				c.mu.RUnlock()
				if handler != nil {
					handler.receive(r.QueryReply.Value)
				}
			}
		case *protocol.ClientMessage_QueryClose:
			if r.QueryClose.ViewID.SessionNum != c.config.SessionId {
				continue
			}
			c.logger.ReceiveFrom("QueryReply", r.QueryClose, member)
			c.mu.Lock()
			handler := c.queries[r.QueryClose.MessageNum]
			delete(c.queries, r.QueryClose.MessageNum)
			c.mu.Unlock()
			if handler != nil {
				handler.complete()
			}
		}
	}
}

// Write sends a write operation to the cluster
func (c *Client) Write(ctx context.Context, in []byte, ch chan<- node.Output) error {
	c.writes <- &requestContext{
		ctx:   ctx,
		value: in,
		ch:    ch,
	}
	return nil
}

// Read sends a read operation to the cluster
func (c *Client) Read(ctx context.Context, in []byte, ch chan<- node.Output) error {
	c.reads <- &requestContext{
		ctx:   ctx,
		value: in,
		ch:    ch,
	}
	return nil
}

func (c *Client) processWrites() {
	for context := range c.writes {
		handler := c.newCommandHandler(context.ch)
		messageID := c.commandID + 1
		c.commandID = messageID
		c.mu.Lock()
		c.commands[messageID] = handler
		c.mu.Unlock()

		c.enqueueWrite(&protocol.CommandRequest{
			SessionNum: c.config.SessionId,
			MessageNum: messageID,
			Timestamp:  time.Now(),
			Value:      context.value,
		})
	}
}

func (c *Client) enqueueWrite(request *protocol.CommandRequest) {
	element := c.writeChans.Front()
	for element != nil {
		ch := element.Value.(chan *protocol.CommandRequest)
		ch <- request
		element = element.Next()
	}
}

func (c *Client) sendWrites(member MemberID, stream protocol.ClientService_ClientStreamClient, ch chan *protocol.CommandRequest) {
	for request := range ch {
		c.logger.SendTo("CommandRequest", request, member)
		_ = stream.Send(&protocol.ClientMessage{
			Message: &protocol.ClientMessage_Command{
				Command: request,
			},
		})
	}
}

func (c *Client) processReads() {
	for context := range c.reads {
		handler := &queryHandler{ch: context.ch}
		messageID := c.queryID + 1
		c.queryID = messageID
		c.mu.Lock()
		c.queries[messageID] = handler
		c.mu.Unlock()

		c.enqueueRead(&protocol.QueryRequest{
			SessionNum: c.config.SessionId,
			MessageNum: messageID,
			Timestamp:  time.Now(),
			Value:      context.value,
		})
	}
}

func (c *Client) enqueueRead(request *protocol.QueryRequest) {
	element := c.readChans.Front()
	for element != nil {
		ch := element.Value.(chan *protocol.QueryRequest)
		ch <- request
		element = element.Next()
	}
}

func (c *Client) sendReads(member MemberID, stream protocol.ClientService_ClientStreamClient, ch chan *protocol.QueryRequest) {
	for request := range ch {
		c.logger.SendTo("QueryRequest", request, member)
		_ = stream.Send(&protocol.ClientMessage{
			Message: &protocol.ClientMessage_Query{
				Query: request,
			},
		})
	}
}

type requestContext struct {
	ctx   context.Context
	value []byte
	ch    chan<- node.Output
}

func (c *Client) newCommandHandler(ch chan<- node.Output) *commandHandler {
	return &commandHandler{
		values: list.New(),
		ch:     ch,
	}
}

// commandHandler is a quorum reply handler
type commandHandler struct {
	values *list.List
	ch     chan<- node.Output
}

func (h *commandHandler) receive(slotNum protocol.LogSlotID, value []byte) {
	slotIndex := uint64(slotNum)
	element := h.values.Back()
	if element != nil {
		slot := element.Value.(*list.List)
		output := slot.Front().Value.(node.Output)
		if output.Index == slotIndex {
			slot.PushBack(node.Output{
				Index: slotIndex,
				Value: value,
			})
			return
		}
	}
	slot := list.New()
	slot.PushBack(node.Output{
		Index: slotIndex,
		Value: value,
	})
	h.values.PushBack(slot)
}

func (h *commandHandler) commit(slotNum protocol.LogSlotID) {
	slotIndex := uint64(slotNum)
	element := h.values.Front()
	for element != nil {
		slot := element.Value.(*list.List)
		slotElement := slot.Front()
		for slotElement != nil {
			output := slotElement.Value.(node.Output)
			if output.Index > slotIndex {
				return
			}
			h.ch <- output
			slotElement = slotElement.Next()
		}
		next := element.Next()
		h.values.Remove(element)
		element = next
	}
}

func (h *commandHandler) complete() {
	close(h.ch)
}

// queryHandler is a query reply handler
type queryHandler struct {
	ch chan<- node.Output
}

func (h *queryHandler) receive(value []byte) {
	h.ch <- node.Output{
		Value: value,
	}
}

func (h *queryHandler) complete() {
	close(h.ch)
}

// Close closes the client
func (c *Client) Close() error {
	return nil
}
