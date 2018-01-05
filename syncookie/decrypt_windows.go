// +build  windows

package main

import (
	"fmt"
	"log"
	"os/user"
	"syscall"
	"unsafe"
)

const (
	CRYPTPROTECT_UI_FORBIDDEN = 0x1
)

var (
	dllcrypt32  = syscall.NewLazyDLL("Crypt32.dll")
	dllkernel32 = syscall.NewLazyDLL("Kernel32.dll")

	procEncryptData = dllcrypt32.NewProc("CryptProtectData")
	procDecryptData = dllcrypt32.NewProc("CryptUnprotectData")
	procLocalFree   = dllkernel32.NewProc("LocalFree")
)

type DATA_BLOB struct {
	cbData uint32
	pbData *byte
}

func NewBlob(d []byte) *DATA_BLOB {
	if len(d) == 0 {
		return &DATA_BLOB{}
	}
	return &DATA_BLOB{
		pbData: &d[0],
		cbData: uint32(len(d)),
	}
}

func (b *DATA_BLOB) ToByteArray() []byte {
	d := make([]byte, b.cbData)
	copy(d, (*[1 << 30]byte)(unsafe.Pointer(b.pbData))[:])
	return d
}

func Encrypt(data []byte) ([]byte, error) {
	var outblob DATA_BLOB
	r, _, err := procEncryptData.Call(uintptr(unsafe.Pointer(NewBlob(data))), 0, 0, 0, 0, 0, uintptr(unsafe.Pointer(&outblob)))
	if r == 0 {
		return nil, err
	}
	defer procLocalFree.Call(uintptr(unsafe.Pointer(outblob.pbData)))
	return outblob.ToByteArray(), nil
}

func Decrypt(data []byte) ([]byte, error) {
	var outblob DATA_BLOB
	r, _, err := procDecryptData.Call(uintptr(unsafe.Pointer(NewBlob(data))), 0, 0, 0, 0, CRYPTPROTECT_UI_FORBIDDEN, uintptr(unsafe.Pointer(&outblob)))
	if r == 0 {
		return nil, err
	}
	defer procLocalFree.Call(uintptr(unsafe.Pointer(outblob.pbData)))
	return outblob.ToByteArray(), nil
}

func decryptValue(encryptedValue []byte) string {
	// key := pbkdf2.Key([]byte(password), []byte(salt), iterations, length, sha1.New)
	// block, err := aes.NewCipher(key)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// decrypted := make([]byte, len(encryptedValue))
	// cbc := cipher.NewCBCDecrypter(block, []byte(iv))
	// cbc.CryptBlocks(decrypted, encryptedValue)

	// plainText, err := aesStripPadding(decrypted)
	// if err != nil {
	// 	fmt.Println("Error decrypting:", err)
	// 	return ""
	// }
	// return string(plainText)
	v, err := Decrypt(encryptedValue)
	if nil != err {
		log.Printf("####decrypt %d bytes %v", len(encryptedValue), err)
	}
	return string(v)

}

func getChromeCookieFile() string {
	usr, _ := user.Current()
	return fmt.Sprintf("%s\\AppData\\Local\\Google\\Chrome\\User Data\\Default\\Cookies", usr.HomeDir)
}
