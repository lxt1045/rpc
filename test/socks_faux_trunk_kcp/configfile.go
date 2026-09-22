package socks_faux_kcp

import (
	"embed"
	"fmt"
	"os"

	"github.com/lxt1045/utils/config"
)

// LoadConfig 加载配置：**优先读磁盘上的 path**（便于容器/运维挂载覆盖配置，
// 不必重建镜像），读不到再回退到编译期嵌入的默认配置（filesystem.Static）。
// 返回配置来源描述，供启动日志展示。
func LoadConfig(path string, fsys embed.FS, conf interface{}) (source string, err error) {
	if bs, rerr := os.ReadFile(path); rerr == nil {
		if err = config.Unmarshal(bs, conf); err != nil {
			return "", fmt.Errorf("parse %s: %w", path, err)
		}
		return "file:" + path, nil
	}
	if err = config.UnmarshalFS(path, fsys, conf); err != nil {
		return "", err
	}
	return "embedded:" + path, nil
}
