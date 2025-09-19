package mesh

import (
	"fmt"

	"github.com/nats-io/nats-server/v2/server"
)

type LeafOptions struct {
	serverName  string
	username    string
	password    string
	leafRemotes [][]string
}

func NewLeafOptions(name string) *LeafOptions {
	return &LeafOptions{
		serverName: name,
	}
}

func (opt *LeafOptions) WithLeafAuth(username, password string) *LeafOptions {
	opt.username = username
	opt.password = password
	return opt
}

func (opt *LeafOptions) WithLeafRemotes(remotes ...[]string) *LeafOptions {
	opt.leafRemotes = remotes
	return opt
}

func (opt *LeafOptions) toNATSConfig() *server.Options {
	leafRemotes := make([]*server.RemoteLeafOpts, 0, len(opt.leafRemotes))
	for _, r := range opt.leafRemotes {
		leafRemotes = append(leafRemotes, &server.RemoteLeafOpts{
			URLs: strsToURLs(r),
		})
	}

	return &server.Options{
		ServerName: opt.serverName,
		DontListen: true,
		LeafNode: server.LeafNodeOpts{
			Username: opt.username,
			Password: opt.password,
			Remotes:  leafRemotes,
		},
	}
}

type Leaf struct {
	conn *conn
}

func NewLeaf(opt *LeafOptions) (*Leaf, error) {
	so := opt.toNATSConfig()
	nc, err := newConn(so)
	if err != nil {
		return nil, fmt.Errorf("failed to create nats connection: %w", err)
	}

	return &Leaf{
		conn: nc,
	}, nil
}
