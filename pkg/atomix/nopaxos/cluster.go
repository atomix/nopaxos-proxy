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
	"context"
	"fmt"
	node "github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/protocol"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/util"
	"google.golang.org/grpc"
	"math"
	"sort"
	"sync"
	"time"
)

const retryPeriod = 100 * time.Millisecond

// MemberID is a member identifier
type MemberID string

// NewCluster returns a new Cluster with the given configuration
func NewCluster(config node.Cluster) *Cluster {
	locations := make(map[MemberID]node.Member)
	memberIDs := make([]MemberID, 0, len(config.Members))
	for id, member := range config.Members {
		locations[MemberID(id)] = member
		memberIDs = append(memberIDs, MemberID(id))
	}
	sort.Slice(memberIDs, func(i, j int) bool {
		return memberIDs[i] < memberIDs[j]
	})
	quorum := int(math.Floor(float64(len(memberIDs))/2.0)) + 1
	return &Cluster{
		member:    MemberID(config.MemberID),
		memberIDs: memberIDs,
		locations: locations,
		conns:     make(map[MemberID]*grpc.ClientConn),
		streams:   make(map[MemberID]protocol.ClientService_ClientStreamClient),
		quorum:    quorum,
	}
}

// Cluster manages the NOPaxos cluster configuration
type Cluster struct {
	member    MemberID
	memberIDs []MemberID
	locations map[MemberID]node.Member
	conns     map[MemberID]*grpc.ClientConn
	streams   map[MemberID]protocol.ClientService_ClientStreamClient
	quorum    int
	mu        sync.RWMutex
}

func (c *Cluster) Member() MemberID {
	return c.member
}

func (c *Cluster) Members() []MemberID {
	return c.memberIDs
}

func (c *Cluster) QuorumSize() int {
	return c.quorum
}

func (c *Cluster) getConn(member MemberID) (*grpc.ClientConn, error) {
	conn, ok := c.conns[member]
	if !ok {
		location, ok := c.locations[member]
		if !ok {
			return nil, fmt.Errorf("unknown member %s", member)
		}

		conn, err := grpc.Dial(
			fmt.Sprintf("%s:%d", location.Host, location.Port),
			grpc.WithInsecure(),
			grpc.WithStreamInterceptor(util.RetryingStreamClientInterceptor(retryPeriod)))
		if err != nil {
			return nil, err
		}
		c.conns[member] = conn
		return conn, nil
	}
	return conn, nil
}

func (c *Cluster) GetStream(member MemberID) (protocol.ClientService_ClientStreamClient, error) {
	c.mu.RLock()
	stream, ok := c.streams[member]
	c.mu.RUnlock()
	if !ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		stream, ok = c.streams[member]
		if !ok {
			conn, err := c.getConn(member)
			if err != nil {
				return nil, err
			}
			client := protocol.NewClientServiceClient(conn)
			stream, err = client.ClientStream(context.Background())
			if err != nil {
				return nil, err
			}
			c.streams[member] = stream
		}
	}
	return stream, nil
}
