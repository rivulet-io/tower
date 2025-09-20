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
	mesh     mesh.WrapConn
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
		t.mesh = clusterConn
	}

	if opt.Leaf.IsSome() {
		leafOpt := opt.Leaf.Unwrap()
		leafConn, err := mesh.NewLeaf(&leafOpt)
		if err != nil {
			operator.Close()
			return nil, err
		}
		t.mesh = leafConn
	}

	if opt.Client.IsSome() {
		clientOpt := opt.Client.Unwrap()
		clientConn, err := mesh.NewClient(&clientOpt)
		if err != nil {
			operator.Close()
			return nil, err
		}
		t.mesh = clientConn
	}

	return t, nil
}

func (t *Tower) Close() error {
	return t.operator.Close()
}

func (t *Tower) Mesh() mesh.WrapConn {
	return t.mesh
}

func (t *Tower) Op() *op.Operator {
	return t.operator
}
