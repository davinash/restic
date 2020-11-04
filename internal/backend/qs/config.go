package qs

import (
	"github.com/pkg/errors"
	"strings"
)

type Config struct {
	Prefix string
}

// NewConfig returns a new Config with the default values filled in.
func NewConfig() Config {
	return Config{}
}

func ParseConfig(s string) (interface{}, error) {
	if !strings.HasPrefix(s, "qs:") {
		return nil, errors.New("invalid qs backend specification")
	}
	return NewConfig(), nil
}
