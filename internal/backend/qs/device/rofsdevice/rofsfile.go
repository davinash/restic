package rofsdevice

import (
	"errors"
	"github.com/restic/restic/internal/backend/qs/rdamanagement/rofs"
	"github.com/restic/restic/internal/debug"
	"io"
)

type RofsFile struct {
	readerOff uint64
	file      *rofs.FileV1
	io.Writer
	io.Reader
	io.WriterAt
	io.ReaderAt
	io.WriteCloser
}

func open(rofs *rofs.Rofs, path string, mode rofs.FileMode) (*RofsFile, error) {
	var err error
	rofsFile := &RofsFile{}
	rofsFile.readerOff = 0
	rofsFile.file, err = rofs.OpenFile(path, mode)

	return rofsFile, err
}

func (rf *RofsFile) Close() error {
	return rf.file.Close()
}

func (rf *RofsFile) Write(b []byte) (int, error) {

	writerOff, err := rf.file.FileSize()
	if err != nil {
		debug.Log("error getting size: %s at: %d ", rf.file.Name(), writerOff)
		return 0, err
	}
	var bytesWritten uint64 = 0

	if len(b) > 0 {
		bytesWritten, err = rf.file.Write(b, writerOff)
		if err != nil {
			debug.Log("error writing: %s at: %d ", rf.file.Name(), writerOff)
			return 0, err
		}
	}

	if bytesWritten != uint64(len(b)) {
		debug.Log("short write on rofs file: %s expected: %d actual %d",
			rf.file.Name(), len(b), bytesWritten)
		return int(bytesWritten), errors.New("short write on rofs file")
	}
	return int(bytesWritten), nil
}

func (rf *RofsFile) Read(b []byte) (int, error) {
	var err error
	if b == nil {
		length, err := rf.file.FileSize()
		if err == nil {
			debug.Log("could not get the size of file: %s with error: %v", rf.file.Name(), err)
			return 0, err
		}
		b = make([]byte, length)
	}
	bytesRead, err := rf.file.ReadAt(b, rf.readerOff)
	if err != nil && err != io.EOF {
		debug.Log("error reading file: %s, error: %v", rf.file.Name(), err)
		return int(bytesRead), err
	}
	if err == io.EOF || bytesRead == 0 {
		err = io.EOF
	}
	rf.readerOff += bytesRead
	return int(bytesRead), err
}

func (rf *RofsFile) WriteAt(b []byte, off int64) (int, error) {
	bytesWritten, err := rf.file.Write(b, uint64(off))
	if err != nil {
		debug.Log("error writing: %s at: %d ", rf.file.Name(), off)
		return 0, err
	}
	if bytesWritten != uint64(len(b)) {
		debug.Log("short write on rofs file: %s expected: %d actual %d",
			rf.file.Name(), len(b), bytesWritten)
		return int(bytesWritten), errors.New("short write on rofs file")
	}
	return int(bytesWritten), nil
}

func (rf *RofsFile) ReadAt(b []byte, off int64) (int, error) {
	bytesRead, err := rf.file.ReadAt(b, uint64(off))
	if err != nil {
		debug.Log("error reading: %s from: %d to: %d", rf.file.Name(), off, len(b))
		return 0, err
	}
	return int(bytesRead), err
}

func (rf *RofsFile) FileSize() (uint64, error) {
	return rf.file.FileSize()
}
