package core

import (
	"bytes"
	"crypto/aes"
	"encoding/base64"

	"github.com/HowXu/gosql/log"
)

//
// Author: http://liuqh.icu/2021/06/19/go/package/16-aes/
//

// AES加密
func EncryptAES(plainText string, key string, cn chan string) error {
	cn <- aesEncryptByECB(plainText, key)
	return nil
}

// AES解密
func DecryptAES(encryptText string, key string, cn chan string) error {
	cn <- aesDecryptByECB(encryptText, key)
	return nil
}

// 加密
func aesEncryptByECB(data, key string) string {
	// 判断key长度
	keyLenMap := map[int]struct{}{16: {}, 24: {}, 32: {}}
	if _, ok := keyLenMap[len(key)]; !ok {
		log.ALL_ERR("Unsupported key len")
		return ""
	}
	// 密钥和待加密数据转成[]byte
	originByte := []byte(data)
	keyByte := []byte(key)
	// 创建密码组，长度只能是16、24、32字节
	block, _ := aes.NewCipher(keyByte)
	// 获取密钥长度
	blockSize := block.BlockSize()
	// 补码
	originByte = pKCS7Padding(originByte, blockSize)
	// 创建保存加密变量
	encryptResult := make([]byte, len(originByte))
	// CEB是把整个明文分成若干段相同的小段，然后对每一小段进行加密
	for bs, be := 0, blockSize; bs < len(originByte); bs, be = bs+blockSize, be+blockSize {
		block.Encrypt(encryptResult[bs:be], originByte[bs:be])
	}
	return base64.StdEncoding.EncodeToString(encryptResult)
}

// 补码
func pKCS7Padding(originByte []byte, blockSize int) []byte {
	// 计算补码长度
	padding := blockSize - len(originByte)%blockSize
	// 生成补码
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	// 追加补码
	return append(originByte, padText...)
}

// 解密
func aesDecryptByECB(data, key string) string {
	// 判断key长度
	keyLenMap := map[int]struct{}{16: {}, 24: {}, 32: {}}
	if _, ok := keyLenMap[len(key)]; !ok {
		log.ALL_ERR("Unsupported key len")
		return ""
	}
	// 反解密码base64
	originByte, _ := base64.StdEncoding.DecodeString(data)
	// 密钥和待加密数据转成[]byte
	keyByte := []byte(key)
	// 创建密码组，长度只能是16、24、32字节
	block, _ := aes.NewCipher(keyByte)
	// 获取密钥长度
	blockSize := block.BlockSize()
	// 创建保存解密变量
	decrypted := make([]byte, len(originByte))
	for bs, be := 0, blockSize; bs < len(originByte); bs, be = bs+blockSize, be+blockSize {
		block.Decrypt(decrypted[bs:be], originByte[bs:be])
	}
	// 解码
	return string(pKCS7UNPadding(decrypted))
}

// 解码
func pKCS7UNPadding(originDataByte []byte) []byte {
	length := len(originDataByte)
	unpadding := int(originDataByte[length-1])
	return originDataByte[:(length - unpadding)]
}
