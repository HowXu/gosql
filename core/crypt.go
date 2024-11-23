package core

import (
	//"github.com/deatil/go-cryptobin/cryptobin/crypto"
)

//TODO 加密

// AES加密
func EncryptAES(plainText string, key string, cn chan string) error {

	// 使用AES-CBC模式和PKCS7填充加密
	// cn <- crypto.
	// 	FromString(plainText).
	// 	SetKey(key).
	// 	Aes().
	// 	ECB().
	// 	PKCS7Padding().
	// 	Encrypt().
	// 	ToBase64String()
	cn <- plainText
	return nil
}

// AES解密
func DecryptAES(encryptText string, key string, cn chan string) error {
	// cn <- crypto.
	// 	FromBase64String(encryptText).
	// 	SetKey(key).
	// 	Aes().
	// 	ECB().
	// 	PKCS7Padding().
	// 	Decrypt().
	// 	ToString()
	cn <- encryptText
	return nil
}
