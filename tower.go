package tower

import (
	"github.com/rivulet-io/tower/mesh"
	"github.com/rivulet-io/tower/op"
	"github.com/rivulet-io/tower/util/monad"
)

type Options struct {
	Operator op.Options                          `json:"operator" yaml:"operator" toml:"operator"`
	Cluster  monad.Optional[mesh.ClusterOptions] `json:"cluster" yaml:"cluster" toml:"cluster"`
	Leaf     monad.Optional[mesh.LeafOptions]    `json:"leaf" yaml:"leaf" toml:"leaf"`
	Client   monad.Optional[mesh.ClientOptions]  `json:"client" yaml:"client" toml:"client"`
}

type Tower struct {
	operator *op.Operator
	cluster  mesh.WrapConn
}

func NewTower(opt *Options) (*Tower, error) {
	operator, err := op.NewOperator(&opt.Operator)
	if err != nil {
		return nil, err
	}

	t := &Tower{
		operator: operator,
	}

	if opt.Cluster.IsSome() {
		clusterOpt := opt.Cluster.Unwrap()
		clusterConn, err := mesh.NewCluster(&clusterOpt)
		if err != nil {
			operator.Close()
			return nil, err
		}
		t.cluster = clusterConn
	}

	if opt.Leaf.IsSome() {
		leafOpt := opt.Leaf.Unwrap()
		leafConn, err := mesh.NewLeaf(&leafOpt)
		if err != nil {
			operator.Close()
			return nil, err
		}
		t.cluster = leafConn
	}

	if opt.Client.IsSome() {
		clientOpt := opt.Client.Unwrap()
		clientConn, err := mesh.NewClient(&clientOpt)
		if err != nil {
			operator.Close()
			return nil, err
		}
		t.cluster = clientConn
	}

	return t, nil
}

func (t *Tower) Close() error {
	return t.operator.Close()
}
