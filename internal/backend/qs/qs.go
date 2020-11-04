package qs

import (
	"context"
	"github.com/pkg/errors"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/restic"
	"io"
	"path"
)

type Backend struct {
	backend.Layout
}

// Ensure that *Backend implements restic.Backend.
var _ restic.Backend = &Backend{}

func Create(cfg Config) (restic.Backend, error) {
	be, err := open(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "open")
	}
	return be, nil
}

func open(cfg Config) (*Backend, error) {
	return &Backend{
		Layout: &backend.DefaultLayout{
			Path: cfg.Prefix,
			Join: path.Join,
		}}, nil
}

func (be *Backend) Location() string {
	panic("implement me")
}

func (be *Backend) Test(ctx context.Context, h restic.Handle) (bool, error) {
	panic("implement me")
}

func (be *Backend) Remove(ctx context.Context, h restic.Handle) error {
	panic("implement me")
}

func (be *Backend) Close() error {
	panic("implement me")
}

func (b Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	panic("implement me")
}

func (be *Backend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	panic("implement me")
}

func (be *Backend) Stat(ctx context.Context, h restic.Handle) (restic.FileInfo, error) {
	panic("implement me")
}

func (be *Backend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {
	panic("implement me")
}

func (be *Backend) IsNotExist(err error) bool {
	panic("implement me")
}

func (be *Backend) Delete(ctx context.Context) error {
	panic("implement me")
}
