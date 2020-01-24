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
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/node"
)

// NewProtocol returns a new NOPaxos Protocol instance
func NewProtocol(config *SequencerConfig) *Protocol {
	return &Protocol{
		config: config,
	}
}

// Protocol is an implementation of the Client interface providing the NOPaxos consensus protocol
type Protocol struct {
	node.Protocol
	config *SequencerConfig
	client *Client
}

// Start starts the NOPaxos protocol
func (p *Protocol) Start(config cluster.Cluster, registry *node.Registry) error {
	client, err := NewClient(config, p.config)
	if err != nil {
		return err
	}
	p.client = client
	return nil
}

// Client returns the NOPaxos sequencer client
func (p *Protocol) Client() node.Client {
	return p.client
}

// Stop stops the NOPaxos protocol
func (p *Protocol) Stop() error {
	return p.client.Close()
}
