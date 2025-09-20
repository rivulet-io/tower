package mesh

import "fmt"

type ClientOptions struct {
	servers  []string
	username string
	password string
}

func NewClientOptions() *ClientOptions {
	return &ClientOptions{}
}

func (opt *ClientOptions) WithServers(servers ...string) *ClientOptions {
	opt.servers = servers
	return opt
}

func (opt *ClientOptions) WithAuth(username, password string) *ClientOptions {
	opt.username = username
	opt.password = password
	return opt
}

type Client struct {
	nc *conn
}

func NewClient(opt *ClientOptions) (*Client, error) {
	nc, err := newClientConn(opt.servers, opt.username, opt.password)
	if err != nil {
		return nil, fmt.Errorf("failed to create nats client connection: %w", err)
	}

	return &Client{
		nc: nc,
	}, nil
}

func (c *Client) Close() {
	if c.nc != nil {
		c.nc.Close()
	}
}
