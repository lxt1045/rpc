package cert

import (
	"log"
	"os"
	"testing"

	"github.com/lxt1045/utils/cert"
)

func TestMake(t *testing.T) {
	dir := "./ca"
	os.Mkdir(dir, 0666) 

	cert.MakeRoot(dir, "lxt")          // 创建根正式
	cert.MainInner(dir, "lxt", "root") // 创建证书颁发机构证书
	cert.MainLeaf(dir, "root", "server", []string{}, []string{"speedtest.cn"})
	cert.MainLeaf(dir, "root", "client", []string{}, []string{})

	log.Println("success...")
}
