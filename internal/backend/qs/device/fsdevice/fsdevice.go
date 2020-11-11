package fsdevice

import "C"
import (
	deivceinterface "github.com/restic/restic/internal/backend/qs/device/deviceinterface"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

const (
	FS_MAX_STREAMS    = 20
	FS_MAX_OPEN_FILES = 5
)

type FsSession struct {
	fileSem  *deivceinterface.OpenFilesCtrl
	refCount int
}

var storageDevices = make(map[string]*FsSession)

type Fsdevice struct {
	path               string
	activeStorageGroup string
	activeContainer    string
	storageGroups      map[string]string
	fsSem              *deivceinterface.OpenFilesCtrl
	sessionMutex       sync.Mutex
}

func NewFsDevice(rootPath string, storage string, container string) *Fsdevice {
	fsdevice := &Fsdevice{
		path:               rootPath,
		activeStorageGroup: storage,
		activeContainer:    container,
		storageGroups:      nil,
		fsSem:              deivceinterface.NewOpenFilesCtrl(FS_MAX_OPEN_FILES),
	}
	return fsdevice
}

func (f *Fsdevice) Connect(user string, password string) error {
	defer f.sessionMutex.Unlock()
	f.sessionMutex.Lock()
	if fsSession, ok := storageDevices[f.path]; ok {
		f.fsSem = fsSession.fileSem
		fsSession.refCount++
		debug.Log("same storage device session already opened: %s refCount: %d", f.path,
			fsSession.refCount)
	} else {
		dirs, err := ioutil.ReadDir(f.path)
		f.storageGroups = make(map[string]string)
		if err == nil {
			for _, entry := range dirs {
				if entry.IsDir() == true {
					f.storageGroups[entry.Name()] = entry.Name()
				}
			}
		} else {
			return errors.New("URL/ Path does not exist ")
		}
		debug.Log("new storage device session opened: %s", f.path)
		f.fsSem = deivceinterface.NewOpenFilesCtrl(FS_MAX_OPEN_FILES)
		fsSession := &FsSession{
			fileSem:  f.fsSem,
			refCount: 1,
		}
		storageDevices[f.path] = fsSession
	}
	return nil
}

func (f *Fsdevice) AddStorageGroup(name string) error {
	_, ok := f.storageGroups[name]
	var err error
	if ok {
		err = errors.New("Storage group exist")
	} else {
		err = os.MkdirAll(filepath.Join(f.path, name), 0755)
		if err == nil {
			f.storageGroups[name] = name
		}
	}
	return err
}

func (f *Fsdevice) RemoveStorageGroup(name string) error {
	_, ok := f.storageGroups[name]
	var err error
	if !ok {
		err = errors.New("Storage group does not exist")
	} else {
		err = os.RemoveAll(filepath.Join(f.path, f.storageGroups[name]))
		if err == nil {
			delete(f.storageGroups, name)
		}
	}
	return err
}

func (f *Fsdevice) ListStorageGroups() ([]string, error) {
	s := make([]string, len(f.storageGroups))
	i := 0
	for _, group := range f.storageGroups {
		s[i] = group
		i++
	}
	return s, nil
}

func (f *Fsdevice) AddContainer(name string) error {
	var err error
	if f.activeStorageGroup != "" {
		retVal, err := f.isContainerExist(name)
		if err != nil {
			return err
		}
		if !retVal {
			err = os.MkdirAll(filepath.Join(f.path, f.activeStorageGroup, name), 0755)
		} else {
			err = errors.New("Container already exist")
		}
	} else {
		err = errors.New("Active Storage group is not set")
	}
	return err
}

func (f *Fsdevice) RemoveContainer(name string) error {
	retVal, err := f.isContainerExist(name)
	if retVal {
		err = os.RemoveAll(filepath.Join(f.path, f.activeStorageGroup, name))
	}
	return err
}

func (f *Fsdevice) isContainerExist(name string) (bool, error) {
	var err error
	retVal := false
	if f.activeStorageGroup != "" {
		if _, err := os.Stat(filepath.Join(f.path, f.activeStorageGroup, name)); os.IsNotExist(err) {
			retVal = false
		} else {
			retVal = true
		}
	} else {
		err = errors.New("No active storage group is set")
	}
	return retVal, err
}

func (f *Fsdevice) GetContainers() ([]string, error) {
	var err error
	var containers []string
	if f.activeStorageGroup != "" {
		dirs, err := ioutil.ReadDir(filepath.Join(f.path, f.activeStorageGroup))
		if err == nil {
			containers = make([]string, len(dirs))
			i := 0
			for _, entry := range dirs {
				if entry.IsDir() == true {
					containers[i] = entry.Name()
					i++
				}
			}
		}
	} else {
		err = errors.New("No Active Storage group Set")
	}
	return containers, err
}

func (f *Fsdevice) AddDirectory(path string) error {
	d := filepath.Join(f.path, f.activeStorageGroup, f.activeContainer, path)
	return os.MkdirAll(d, 0755)
}

func (f *Fsdevice) RemoveDir(dirPath string) error {
	return os.RemoveAll(filepath.Join(f.path, f.activeStorageGroup, f.activeContainer, dirPath))
}

func (f *Fsdevice) getDevicePath(device string) (string, error) {
	var devicepath string
	retVal, err := f.isContainerExist(device)
	if err == nil && retVal == true {
		devicepath = filepath.Join(f.path, f.activeStorageGroup, device)
	} else {
		err = errors.New("Container does not exist")
	}
	return devicepath, err
}

func (f *Fsdevice) DeleteFile(path string) error {
	return os.RemoveAll(filepath.Join(f.path, f.activeStorageGroup, f.activeContainer, path))
}

func (f *Fsdevice) ObjectExist(path string) (bool, error) {
	devicePath, err := f.getDevicePath(f.activeContainer)
	if _, err = os.Stat(filepath.Join(devicePath, path)); os.IsNotExist(err) {
		return false, err
	} else {
		return true, err
	}
}

func (f *Fsdevice) DirWalk(path string) ([]deivceinterface.DeviceFileInfo, error) {
	dirList, err := ioutil.ReadDir(filepath.ToSlash(filepath.Join(f.path, f.activeStorageGroup, f.activeContainer,
		path)))
	if err != nil {
		return nil, err
	}
	var values []deivceinterface.DeviceFileInfo
	for _, dir := range dirList {
		values = append(values, deivceinterface.DeviceFileInfo{Name: dir.Name(), IsDir: dir.IsDir()})
	}
	return values, err
}

func (f *Fsdevice) Disconnect() {
	defer f.sessionMutex.Unlock()
	f.sessionMutex.Lock()
	fsSession := storageDevices[f.path]
	if fsSession != nil {
		fsSession.refCount--
		if fsSession.refCount == 0 {
			delete(storageDevices, f.path)
			f.path = ""
			f.activeContainer = ""
			f.activeStorageGroup = ""
		}
	}
}

func (f *Fsdevice) GetMaxStreams() int {
	return FS_MAX_STREAMS
}

func (f *Fsdevice) GetBlockSize() uint64 {
	return uint64(4 * 1024 * 1024) // Assumed 4 MB
}

func (f *Fsdevice) CopyFileFromDevice(sourceFile string, destFile string) error {
	fpDest, err := os.OpenFile(destFile, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer fpDest.Close()
	if err != nil {
		debug.Log("error opening dest file %s: %v", destFile, err)
		return err
	}

	fpSource, err := f.OpenRead(sourceFile)
	defer fpSource.(*os.File).Close()
	if err != nil {
		debug.Log("error opening src file %s : %v", sourceFile, err)
		return err
	}

	bytesWritten, err := io.Copy(fpDest, fpSource.(io.Reader))
	if err != nil {
		debug.Log("error in copying %s to %s : %v", sourceFile, destFile, err)
	}
	debug.Log("Bytes copied from %s to %s : %d", sourceFile, destFile, bytesWritten)
	return err
}

func (f *Fsdevice) CopyFileToDevice(sourceFile string, destFile string) error {
	fpDest, err := f.OpenWrite(destFile)
	defer fpDest.(*os.File).Close()
	if err != nil {
		debug.Log("error opening dest file %s: %v", destFile, err)
		return err
	}
	fpSource, err := os.Open(sourceFile)
	defer fpSource.Close()
	if err != nil {
		debug.Log("error opening src file %s: %v", sourceFile, err)
		return err
	}
	bytesWritten, err := io.Copy(fpDest.(io.Writer), fpSource)
	if err != nil {
		debug.Log("error copying %s to %s: %v", sourceFile, destFile, err)
	}
	debug.Log("Bytes copied from %s to %s : %d", sourceFile, destFile, bytesWritten)
	return err
}

func (f *Fsdevice) OpenStreamFileWrite(path string) (interface{}, error) {
	f.fsSem.Wait()
	fh, err := os.OpenFile(filepath.ToSlash(filepath.Join(f.path, f.activeStorageGroup, f.activeContainer, path)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.fsSem.Signal()
		debug.Log("create failed : %v", err)
	}
	return fh, err
}

func (f *Fsdevice) OpenWrite(path string) (interface{}, error) {
	fh, err := os.OpenFile(filepath.ToSlash(filepath.Join(f.path, f.activeStorageGroup, f.activeContainer, path)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		debug.Log("create failed : %v", err)
	}
	return fh, err
}

func (f *Fsdevice) OpenStreamFileRead(path string) (interface{}, error) {
	f.fsSem.Wait()
	fh, err := os.Open(filepath.ToSlash(filepath.Join(f.path, f.activeStorageGroup, f.activeContainer, path)))
	if err != nil {
		f.fsSem.Signal()
		debug.Log("open failed : %v", err)
	}
	return fh, err
}

func (f *Fsdevice) OpenRead(path string) (interface{}, error) {
	fh, err := os.Open(filepath.ToSlash(filepath.Join(f.path, f.activeStorageGroup, f.activeContainer, path)))
	if err != nil {
		debug.Log("open failed : %v", err)
	}
	return fh, err
}

func (f *Fsdevice) ReplicateFile(srcfh deivceinterface.RawFilePtr, dstfh deivceinterface.RawFilePtr, srcOffset uint64,
	length uint64) (interface{}, error) {
	fileReplication := &FileReplication{}
	err := f.fileInclusionStart(srcfh.(*os.File), dstfh.(*os.File), srcOffset, length, fileReplication)
	return fileReplication, err
}

func (f *Fsdevice) GetBytesReplicated(replication interface{}) uint64 {
	return replication.(*FileReplication).bytesReplicated
}

type FileReplication struct {
	oSource         deivceinterface.RawFilePtr
	oDestination    deivceinterface.RawFilePtr
	bytesReplicated uint64
	status          int
}

func (f *Fsdevice) fileInclusionStart(srcfh *os.File, dstfh *os.File, srcOffset uint64, length uint64,
	replication *FileReplication) error {
	replication.oSource = srcfh
	replication.oDestination = dstfh
	b := make([]byte, length)
	_, err := srcfh.ReadAt(b, int64(srcOffset))
	if err != nil {
		return err
	}
	bytesWritten, err := dstfh.Write(b)
	if err == nil {
		replication.status = 1
		replication.bytesReplicated = uint64(bytesWritten)
	}
	return err
}

func (f *Fsdevice) FileSize(fh deivceinterface.RawFilePtr) (uint64, error) {
	stat, err := fh.(*os.File).Stat()
	if err != nil {
		return 0, err
	}
	return uint64(stat.Size()), err
}

func (f *Fsdevice) Close(fileHandle deivceinterface.RawFilePtr) error {
	err := fileHandle.(*os.File).Close()
	f.fsSem.Signal()
	return err
}

func (f *Fsdevice) GetOpenFiles() int {
	return f.fsSem.Counter()
}

func (f *Fsdevice) CopyBufferToDevice(path string, buffer []byte) error {
	fp, err := f.OpenWrite(path)
	defer fp.(*os.File).Close()
	if err != nil {
		debug.Log("error opening dest file %s: %v", path, err)
		return err
	}
	bytesWritten, err := fp.(*os.File).Write(buffer)
	if err != nil {
		debug.Log("could not write buffer to file %s with error : %v", path, err)
	}
	debug.Log("Bytes copied to %s : %d", path, bytesWritten)
	return err
}

func (f *Fsdevice) CopyBufferFromDevice(path string) ([]byte, error) {
	fp, err := f.OpenRead(path)
	defer fp.(*os.File).Close()
	if err != nil {
		debug.Log("error opening dest file %s: %v", path, err)
		return nil, err
	}
	size, err := f.FileSize(fp)
	if err != nil {
		debug.Log("error opening dest file %s: %v", path, err)
		return nil, err
	}
	buffer := make([]byte, size)
	bytesRead, err := fp.(*os.File).Read(buffer)
	if err != nil {
		debug.Log("could not read from file %s with error : %v", path, err)
		return nil, err
	}
	debug.Log("Bytes copied from %s : %d", path, bytesRead)
	return buffer, err
}
