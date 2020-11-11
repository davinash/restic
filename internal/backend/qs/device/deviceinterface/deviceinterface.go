package deivceinterface

const (
	ROFS_DEVICE = iota + 1
	FILESYSTEM_DEVICE
)

type RawFilePtr interface{}

type DeviceAttributes struct {
	Url          string
	Location     string
	Username     string
	Password     string
	Dmainfo      string
	StorageGroup string
	Container    string
	DeviceType   string
}

type DeviceFileInfo struct {
	Name  string
	IsDir bool
}

type DeviceInterface interface {
	Connect(user string, password string) error
	AddDirectory(path string) error
	DirWalk(path string) ([]DeviceFileInfo, error)
	DeleteFile(path string) error
	ObjectExist(path string) (bool, error)
	Disconnect()
	GetMaxStreams() int
	GetBlockSize() uint64
	CopyFileToDevice(sourceFile string, destFile string) error
	CopyFileFromDevice(sourceFile string, destFile string) error
	OpenStreamFileWrite(path string) (interface{}, error)
	OpenStreamFileRead(path string) (interface{}, error)
	ReplicateFile(srcfh RawFilePtr, dstfh RawFilePtr, srcOffset uint64, length uint64) (interface{}, error)
	FileSize(fh RawFilePtr) (uint64, error)
	GetBytesReplicated(replication interface{}) uint64
	Close(fileHandle RawFilePtr) error
	GetOpenFiles() int
	RemoveDir(dirPath string) error
	CopyBufferToDevice(path string, buffer []byte) error
	CopyBufferFromDevice(path string) ([]byte, error)
	OpenRead(path string) (interface{}, error)
}

type OpenFilesCtrl struct {
	sem chan bool
}

func NewOpenFilesCtrl(maxOpenFiles int) *OpenFilesCtrl {
	openFilesSem := &OpenFilesCtrl{}
	openFilesSem.sem = make(chan bool, maxOpenFiles)
	return openFilesSem
}

func (sem *OpenFilesCtrl) Wait() {
	sem.sem <- true
}

func (sem *OpenFilesCtrl) Signal() {
	<-sem.sem
}

func (sem *OpenFilesCtrl) Counter() int {
	return len(sem.sem)
}
