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
	"github.com/atomix/atomix-api/proto/atomix/controller"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/counter"
	node2 "github.com/atomix/atomix-go-node/pkg/atomix/node"
	"github.com/atomix/atomix-go-node/pkg/atomix/registry"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos"
	"github.com/atomix/atomix-nopaxos-node/pkg/atomix/nopaxos/config"
	"github.com/gogo/protobuf/proto"
	"github.com/sirupsen/logrus"
	"sync"
	"testing"
	"time"
)

func BenchmarkProtocol(b *testing.B) {
	logrus.SetLevel(logrus.InfoLevel)

	pingInterval := 1 * time.Second
	leaderTimeout := 2 * time.Second
	config := &config.ProtocolConfig{PingInterval: &pingInterval, LeaderTimeout: &leaderTimeout}

	members := map[string]cluster.Member{
		"foo": {
			ID:   "foo",
			Host: "localhost",
			Port: 5678,
		},
		"bar": {
			ID:   "bar",
			Host: "localhost",
			Port: 5679,
		},
		"baz": {
			ID:   "baz",
			Host: "localhost",
			Port: 5680,
		},
	}

	clusterFoo := cluster.Cluster{
		MemberID: "foo",
		Members:  members,
	}
	serverFoo := nopaxos.NewServer(clusterFoo, registry.Registry, config)

	clusterBar := cluster.Cluster{
		MemberID: "bar",
		Members:  members,
	}
	serverBar := nopaxos.NewServer(clusterBar, registry.Registry, config)

	clusterBaz := cluster.Cluster{
		MemberID: "baz",
		Members:  members,
	}
	serverBaz := nopaxos.NewServer(clusterBaz, registry.Registry, config)

	go serverFoo.Start()
	go serverBar.Start()
	go serverBaz.Start()

	time.Sleep(5 * time.Second)

	c := &controller.PartitionConfig{
		Partition: &controller.PartitionId{
			Partition: 1,
			Group: &controller.PartitionGroupId{
				Name:      "test",
				Namespace: "default",
			},
		},
		Members: []*controller.NodeConfig{
			{
				ID:   "sequencer",
				Host: "localhost",
				Port: 5677,
			},
			{
				ID:   "foo",
				Host: "localhost",
				Port: 5678,
			},
			{
				ID:   "bar",
				Host: "localhost",
				Port: 5679,
			},
			{
				ID:   "baz",
				Host: "localhost",
				Port: 5680,
			},
		},
	}
	protocol := NewProtocol(&SequencerConfig{
		SessionId: 1,
	})
	node := atomix.NewNode("sequencer", c, protocol, registry.Registry)
	node.Start()
	time.Sleep(1 * time.Second)

	bytes, _ := proto.Marshal(&counter.SetRequest{
		Value: 1,
	})
	bytes, _ = proto.Marshal(&service.CommandRequest{
		Context: &service.RequestContext{},
		Name:    "set",
		Command: bytes,
	})
	bytes, _ = proto.Marshal(&service.ServiceRequest{
		Id: &service.ServiceId{
			Type:      "counter",
			Name:      "test",
			Namespace: "test",
		},
		Request: &service.ServiceRequest_Command{
			Command: bytes,
		},
	})

	client := protocol.Client()
	b.Run("BenchmarkWrites", func(b *testing.B) {
		//for i := 0; i < b.N; i++ {
		//	ch := make(chan node2.Output)
		//	go client.Write(context.Background(), bytes, ch)
		//	<-ch
		//}

		ch := make(chan struct{}, 8)
		wg := &sync.WaitGroup{}
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				for range ch {
					ch := make(chan node2.Output)
					_ = client.Write(context.Background(), bytes, ch)
					<-ch
				}
				wg.Done()
			}()
		}

		for n := 0; n < b.N; n++ {
			ch <- struct{}{}
		}
		close(ch)

		wg.Wait()
	})
}
