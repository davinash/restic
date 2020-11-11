package qs

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/qs/device"
	deivceinterface "github.com/restic/restic/internal/backend/qs/device/deviceinterface"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
)

type Backend struct {
	device deivceinterface.DeviceInterface
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

func Open(cfg Config) (restic.Backend, error) {
	return open(cfg)
}

func open(cfg Config) (*Backend, error) {
	dmaInfo := "Restic-1.0.0"
	deviceAttr := deivceinterface.DeviceAttributes{
		Url:          cfg.HostName,
		Username:     cfg.UserName,
		Password:     cfg.Password,
		Dmainfo:      dmaInfo,
		StorageGroup: cfg.StorageGroup,
		Container:    cfg.Container,
		DeviceType:   "ROFS_DEVICE",
	}

	fmt.Println(deviceAttr)

	if token := os.Getenv("QS_DEVICE_TYPE"); token != "" {
		deviceAttr.DeviceType = token
	}

	d, err := device.CheckDeviceConnection(deviceAttr)
	if err != nil {
		panic(err)
		debug.Log("Failed ConnectToDevice with error = %v", err)
		return nil, nil
	}

	return &Backend{
		device: d,
		Layout: &backend.DefaultLayout{
			Path: cfg.Prefix,
			Join: path.Join,
		}}, nil
}

func (be *Backend) Location() string {
	panic("implement me")
}

func (be *Backend) Test(ctx context.Context, h restic.Handle) (bool, error) {
	return false, nil
}

func (be *Backend) Remove(ctx context.Context, h restic.Handle) error {
	panic("implement me")
}

func (be *Backend) Close() error {
	panic("implement me")
}

func (b Backend) Save(ctx context.Context, h restic.Handle, rd restic.RewindReader) error {
	debug.Log("Save %v", h)
	if err := h.Valid(); err != nil {
		return err
	}
	fileName := b.Filename(h)
	fmt.Printf("FileName -> %s\n", fileName)
	fmt.Printf("Dir -> %s\n", filepath.Dir(fileName))
	err := b.device.AddDirectory(filepath.Dir(fileName))
	if err != nil {
		panic(err)
	}
	buf, err := ioutil.ReadAll(rd)
	if err != nil {
		panic(err)
	}
	err = b.device.CopyBufferToDevice(fileName, buf)
	if err != nil {
		panic(err)
	}
	return nil
}

func (be *Backend) Load(ctx context.Context, h restic.Handle, length int, offset int64, fn func(rd io.Reader) error) error {
	panic("implement me")
}

func (be *Backend) Stat(ctx context.Context, h restic.Handle) (restic.FileInfo, error) {
	fileName := be.Filename(h)
	fmt.Printf("Stat -> %s\n", fileName)
	fp, err := be.device.OpenRead(fileName)
	// TO DO : FIX ME
	//defer fp.Close()
	if err != nil {
		panic(err)
	}
	size, err := be.device.FileSize(fp)
	if err != nil {
		panic(err)
	}
	return restic.FileInfo{
		Size: int64(size),
		Name: fileName,
	}, nil
}

func (be *Backend) List(ctx context.Context, t restic.FileType, fn func(restic.FileInfo) error) error {
	dirName, _ := be.Basedir(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fileInfos, err := be.device.DirWalk(dirName)
	if err != nil {
		panic(err)
	}
	for _, file := range fileInfos {
		fi := restic.FileInfo{
			Name: path.Base(file.Name),
		}
		fp, err := be.device.OpenRead(filepath.Join(dirName, file.Name))
		if err != nil {
			panic(err)
		}
		size, err := be.device.FileSize(fp)
		if err != nil {
			panic(err)
		}
		fi.Size = int64(size)

		if ctx.Err() != nil {
			return ctx.Err()
		}

		err = fn(fi)
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return ctx.Err()
}

func (be *Backend) IsNotExist(err error) bool {
	panic("implement me")
}

func (be *Backend) Delete(ctx context.Context) error {
	panic("implement me")
}
