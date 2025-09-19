package mesh

import (
	"net/url"

	"github.com/nats-io/nats-server/v2/server"
)

type RemoteGateways []RemoteGateway

type RemoteGateway struct {
	Name string
	URLs []string
}

func NewRemoteGateways() *RemoteGateways {
	rg := RemoteGateways{}
	return &rg
}

func (rg *RemoteGateways) Add(name string, url ...string) *RemoteGateways {
	*rg = append(*rg, RemoteGateway{
		Name: name,
		URLs: url,
	})

	return rg
}

func (rg *RemoteGateways) Remove(name string) *RemoteGateways {
	for i, r := range *rg {
		if r.Name == name {
			*rg = append((*rg)[:i], (*rg)[i+1:]...)
			return rg
		}
	}

	return rg
}

func (rg *RemoteGateways) toNATSConfig() []*server.RemoteGatewayOpts {
	remotes := make([]*server.RemoteGatewayOpts, 0, len(*rg))
	for _, r := range *rg {
		urls := strsToURLs(r.URLs)
		if len(urls) == 0 {
			continue
		}

		remotes = append(remotes, &server.RemoteGatewayOpts{
			Name: r.Name,
			URLs: urls,
		})
	}

	return remotes
}

func strsToURLs(strs []string) []*url.URL {
	urls := make([]*url.URL, 0, len(strs))
	for _, s := range strs {
		u, err := url.Parse(s)
		if err != nil {
			continue
		}

		urls = append(urls, u)
	}

	return urls
}
