package skynsh

import (
	"fmt"

	"github.com/rivulet-io/tower"
)

type Options struct{}

type Skynsh struct {
	tower *tower.Tower
}

func New(opt *Options) (*Skynsh, error) {
	t, err := tower.NewTower(&tower.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to create tower instance: %w", err)
	}

	return &Skynsh{
		tower: t,
	}, nil
}
