package device

import (
	"errors"
	"fmt"
	deivceinterface "github.com/restic/restic/internal/backend/qs/device/deviceinterface"
	"github.com/restic/restic/internal/backend/qs/device/fsdevice"
	"github.com/restic/restic/internal/backend/qs/device/rofsdevice"
)

//ConnectToDevice connect with the device specified
func ConnectToDevice(deviceAttributes deivceinterface.DeviceAttributes) (deivceinterface.DeviceInterface, error) {
	var device deivceinterface.DeviceInterface
	var err error
	if deviceAttributes.DeviceType == "ROFS_DEVICE" {
		if deviceAttributes.StorageGroup == "" {
			deviceAttributes.StorageGroup = "_system_wide"
		}
		device = rofsdevice.NewRofsDevice(deviceAttributes.Url, deviceAttributes.StorageGroup, deviceAttributes.Container,
			deviceAttributes.Dmainfo)
		err = device.Connect(deviceAttributes.Username, deviceAttributes.Password)
	} else if deviceAttributes.DeviceType == "FILESYSTEM_DEVICE" {
		fmt.Println("Using FILESYSTEM_DEVICE")
		device = fsdevice.NewFsDevice(deviceAttributes.Url, deviceAttributes.StorageGroup, deviceAttributes.Container)
		err = device.Connect(deviceAttributes.Username, deviceAttributes.Password)
	} else {
		err = errors.New("invalid device type specified")
	}
	return device, err
}

func CheckDeviceConnection(deviceAttributes deivceinterface.DeviceAttributes) (deivceinterface.DeviceInterface, error) {
	device, err := ConnectToDevice(deviceAttributes)
	return device, err
}
