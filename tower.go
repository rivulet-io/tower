package tower

import "github.com/rivulet-io/tower/op"

type Options struct {
	Operator op.Options `json:"operator" yaml:"operator" toml:"operator"`
}

type Tower struct {
	operator *op.Operator
}

func NewTower(opt *Options) (*Tower, error) {
	operator, err := op.NewOperator(&opt.Operator)
	if err != nil {
		return nil, err
	}

	return &Tower{
		operator: operator,
	}, nil
}

func (t *Tower) Close() error {
	return t.operator.Close()
}
