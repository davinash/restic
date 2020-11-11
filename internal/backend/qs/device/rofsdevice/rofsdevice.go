package rofsdevice

import "C"
import (
	"fmt"
	"github.com/pkg/errors"
	deivceinterface "github.com/restic/restic/internal/backend/qs/device/deviceinterface"
	"github.com/restic/restic/internal/backend/qs/rdamanagement/rofs"
	"github.com/restic/restic/internal/debug"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	ROFS_MAX_STREAMS    = 20
	ROFS_MAX_OPEN_FILES = 5
)

type RDASession struct {
	fileSem  *deivceinterface.OpenFilesCtrl
	refCount int
}

var storageDevices = make(map[interface{}]*RDASession)

type Rofsdevice struct {
	rofs         *rofs.Rofs
	rofsSem      *deivceinterface.OpenFilesCtrl
	sessionMutex sync.Mutex
}

func NewRofsDevice(url string, activeStorageGroup string, activeContainer string, dmaInfo string) *Rofsdevice {
	rofsDevice := &Rofsdevice{
		rofs: rofs.NewRofs(url, activeStorageGroup, activeContainer, dmaInfo),
	}
	return rofsDevice
}

//Connect connect with QS
func (r *Rofsdevice) Connect(user string, password string) error {
	err := r.rofs.Connect(user, password)
	if err != nil {
		debug.Log(fmt.Sprintf("Error During connect, Error = %v", err))
		return err
	}
	defer r.sessionMutex.Unlock()
	r.sessionMutex.Lock()
	if rdaSession, ok := storageDevices[r.rofs.GetSessionObject()]; ok {
		r.rofsSem = rdaSession.fileSem
		rdaSession.refCount++
		debug.Log("same storage device session already opened: %s refCount: %d",
			r.rofs.GetDeviceUrl(), rdaSession.refCount)
	} else {
		debug.Log("new storage device session opened: %s", r.rofs.GetDeviceUrl())
		r.rofsSem = deivceinterface.NewOpenFilesCtrl(ROFS_MAX_OPEN_FILES)
		rdaSession := &RDASession{
			fileSem:  r.rofsSem,
			refCount: 1,
		}
		storageDevices[r.rofs.GetSessionObject()] = rdaSession
	}
	return nil
}

func (r *Rofsdevice) AddDirectory(path string) error {
	exist, err := r.ObjectExist(filepath.Dir(path))
	if err != nil {
		return err
	}
	if !exist {
		err = r.AddDirectory(filepath.ToSlash(filepath.Dir(path)))
	} else {
		err = r.rofs.AddDirectory(filepath.ToSlash(path))
	}
	return err
}

// RemoveDir Removes the directory from the storage
func (r *Rofsdevice) RemoveDir(dirPath string) error {
	exist, err := r.ObjectExist(filepath.Dir(dirPath))
	if err != nil {
		return err
	}
	if exist {
		err = r.rofs.RemoveAll(filepath.ToSlash(dirPath))
	} else {
		err = errors.New("Directory Path does not exist: " + dirPath)
	}
	return err
}

func (r *Rofsdevice) DeleteFile(path string) error {
	return r.rofs.DeleteFile(filepath.ToSlash(path))
}

func (r *Rofsdevice) CopyFileFromDevice(sourceFile string, destFile string) error {
	fpDest, err := os.OpenFile(destFile, os.O_CREATE|os.O_WRONLY, 755)
	if err != nil {
		debug.Log("could not open dest file %s with error : %v", destFile, err)
		return err
	}
	defer fpDest.Close()
	fpSource, err := r.OpenRead(sourceFile)
	if err != nil {
		debug.Log("could not open src file %s with error : %v", sourceFile, err)
		return err
	}
	defer fpSource.(*RofsFile).Close()
	bytesWritten, err := io.Copy(fpDest, fpSource.(*RofsFile))
	debug.Log("Bytes copied from %s to %s : %d", sourceFile, destFile, bytesWritten)
	return err
}

func (r *Rofsdevice) CopyFileToDevice(sourceFile string, destFile string) error {
	fpDest, err := r.openWrite(destFile)
	if err != nil {
		debug.Log("could not open dest file %s with error : %v", destFile, err)
		return err
	}
	defer fpDest.(*RofsFile).Close()
	fpSource, err := os.Open(sourceFile)
	if err != nil {
		debug.Log("could not open src file %s with error : %v", sourceFile, err)
		return err
	}
	defer fpSource.Close()
	bytesWritten, err := io.Copy(fpDest.(*RofsFile), fpSource)
	debug.Log("Bytes copied from %s to %s : %d", sourceFile, destFile, bytesWritten)
	return err
}

func (r *Rofsdevice) ObjectExist(path string) (bool, error) {
	return r.rofs.ObjectExist(filepath.ToSlash(path))
}

func (r *Rofsdevice) DirWalk(path string) ([]deivceinterface.DeviceFileInfo, error) {
	dirList, err := r.rofs.GetObjects(filepath.ToSlash(path))
	var values []deivceinterface.DeviceFileInfo
	for _, dir := range dirList {
		if dir.Type == rofs.ROFS_DIRENTRY_DIRECTORY {
			values = append(values, deivceinterface.DeviceFileInfo{Name: dir.Name, IsDir: true})
		} else {
			values = append(values, deivceinterface.DeviceFileInfo{Name: dir.Name})
		}
	}
	return values, err
}

func (r *Rofsdevice) Disconnect() {
	defer r.sessionMutex.Unlock()
	r.sessionMutex.Lock()
	rdaSession := storageDevices[r.rofs.GetSessionObject()]
	if rdaSession != nil {
		rdaSession.refCount--
		if rdaSession.refCount == 0 {
			delete(storageDevices, r.rofs.GetSessionObject())
			r.rofs.Disconnect()
		}
	}
}

func (r *Rofsdevice) GetMaxStreams() int {
	return ROFS_MAX_STREAMS // max parallel streams handled by QS is 256
}

func (r *Rofsdevice) GetBlockSize() uint64 {
	return uint64(4 * 1024 * 1024) // Assumed 4 MB
}

func (r *Rofsdevice) OpenStreamFileWrite(path string) (interface{}, error) {
	r.rofsSem.Wait()
	rofsFile, err := r.openWrite(path)
	if err != nil {
		r.rofsSem.Signal()
		debug.Log("Could not open rofs device file: %s with error: %v", path, err)
	}
	return rofsFile, nil
}

func (r *Rofsdevice) OpenStreamFileRead(path string) (interface{}, error) {
	r.rofsSem.Wait()
	rofsFile, err := r.OpenRead(path)
	if err != nil {
		r.rofsSem.Signal()
		debug.Log("Could not open rofs device file: %s with error: %v", path, err)
	}
	return rofsFile, err
}

func (r *Rofsdevice) ReplicateFile(srcfh deivceinterface.RawFilePtr, dstfh deivceinterface.RawFilePtr, srcOffset uint64,
	length uint64) (interface{}, error) {
	rdaReplication := &rofs.RDAReplication{}
	err := r.rofs.FileInclusionV2Start(srcfh.(*RofsFile).file, dstfh.(*RofsFile).file, srcOffset, length, rdaReplication)
	return rdaReplication, err
}

func (r *Rofsdevice) GetBytesReplicated(replication interface{}) uint64 {
	return uint64(replication.(*rofs.RDAReplication).BytesReplicated)
}

func (r *Rofsdevice) FileSize(fh deivceinterface.RawFilePtr) (uint64, error) {
	return fh.(*RofsFile).FileSize()
}

func (r *Rofsdevice) Close(fileHandle deivceinterface.RawFilePtr) error {
	err := fileHandle.(*RofsFile).Close()
	r.rofsSem.Signal()
	return err
}

func (r *Rofsdevice) OpenRead(path string) (interface{}, error) {
	rofsFile, err := open(r.rofs, path, rofs.OPEN_READ)
	return rofsFile, err
}

func (r *Rofsdevice) openWrite(path string) (interface{}, error) {
	exist, err := r.ObjectExist(path)
	if err != nil {
		return nil, err
	}
	var rofsFile *RofsFile
	if !exist {
		rofsFile, err = open(r.rofs, path, rofs.OPEN_CREATE|rofs.OPEN_WRITE)
	} else {
		rofsFile, err = open(r.rofs, path, rofs.OPEN_APPEND)
	}
	if err != nil {
		return nil, err
	}
	return rofsFile, nil
}

func (r *Rofsdevice) GetOpenFiles() int {
	return r.rofsSem.Counter()
}

func (r *Rofsdevice) CopyBufferToDevice(path string, buffer []byte) error {
	fp, err := r.openWrite(path)
	if err != nil {
		debug.Log("could not open file %s with error : %v", path, err)
		return err
	}
	defer fp.(*RofsFile).Close()
	bytesWritten, err := fp.(*RofsFile).Write(buffer)
	if err != nil {
		debug.Log("could not write buffer to file %s with error : %v", path, err)
	}
	debug.Log("Bytes copied to %s : %d", path, bytesWritten)
	return err
}

func (r *Rofsdevice) CopyBufferFromDevice(path string) ([]byte, error) {
	fp, err := r.OpenRead(path)
	if err != nil {
		debug.Log("could not open file %s with error : %v", path, err)
		return nil, err
	}
	defer fp.(*RofsFile).Close()
	var buffer []byte
	bytesRead, err := fp.(*RofsFile).Read(buffer)
	if err != nil {
		debug.Log("could not read from file %s with error : %v", path, err)
		return nil, err
	}
	debug.Log("Bytes copied from %s : %d", path, bytesRead)
	return buffer, err
}
