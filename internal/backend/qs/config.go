package qs

import (
	"fmt"
	"github.com/pkg/errors"
	"strings"
)

type Config struct {
	Prefix       string
	HostName     string
	UserName     string
	StorageGroup string
	Container    string
	Password     string
}

// NewConfig returns a new Config with the default values filled in.
func NewConfig() Config {
	return Config{}
}

func Split(r rune) bool {
	return r == ':' || r == '@' || r == '/'
}

func ParseConfig(s string) (interface{}, error) {
	if !strings.HasPrefix(s, "qs:") {
		return nil, errors.New("invalid qs backend specification")
	}
	tok := strings.FieldsFunc(s[3:], Split)
	fmt.Println(tok)

	return Config{
		Prefix:       "",
		UserName:     tok[0],
		HostName:     "/home/adongre/my_backup_device",
		StorageGroup: "SG1",
		Container:    "C1",
	}, nil
}
