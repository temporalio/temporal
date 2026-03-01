package lib

import (
	"syscall"
)

func Syscall(trap, a1, a2, a3 uintptr) (r1, r2 uintptr, err syscall.Errno) {
	switch trap {
	case syscall.SYS_MUNMAP: // map or unmap files or devices into memory
		return syscall.Syscall(trap, a1, a2, a3)
	default:
		panic("not implemented: syscall.Syscall")
	}
}

func Clearenv() {
	panic("not implemented: syscall.Clearenv")
}

func SysGetenv(key string) (value string, found bool) {
	panic("not implemented: syscall.Getenv")
}

func SysUnsetenv(key string) error {
	panic("not implemented: syscall.Unsetenv")
}

func Exec(argv0 string, argv []string, envv []string) (err error) {
	panic("not implemented: syscall.Exec")
}

func SysRead(fd int, p []byte) (n int, err error) {
	panic("not implemented: syscall.Read")
}

func SysReadlink(path string, buf []byte) (n int, err error) {
	panic("not implemented: syscall.Readlink")
}

func Statfs(path string, buf *syscall.Statfs_t) (err error) {
	panic("not implemented: syscall.Statfs")
}

func Recvfrom(fd int, p []byte, flags int) (n int, from syscall.Sockaddr, err error) {
	panic("not implemented: syscall.Recvfrom")
}

func Getgid() int {
	panic("not implemented: syscall.Getgid")
}

func Symlink(path string, link string) (err error) {
	panic("not implemented: syscall.Symlink")
}
