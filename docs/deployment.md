# 部署说明

- 本机 SSH 别名：`202.165.123.76`，已配置在用户的 `~/.ssh/config` 中。
- 服务器：`202.165.123.76`，SSH 端口 `22333`，用户 `root`。
- 访问地址：<http://202.165.123.76:8000>。
- 部署目录：`/usr/local/etc/invest`。
- Compose 服务名：`invest-app`，容器名：`invest`。
- 线上镜像：`cheedonghu/invest:latest`。
- 数据库配置目录：`/usr/local/etc/invest/config`，挂载到容器 `/config`；部署时保留服务器配置，不上传本机配置或密钥。

## 更新流程

1. 执行相关离线测试，并检查工作区改动。
2. 将当前待部署源码打包到服务器 `releases/<版本号>`，记录包校验值与工作区状态。
3. 为当前线上镜像保留 `cheedonghu/invest:rollback-<版本号>` 标签，备份 Compose 文件。备份与记录保存在对应版本目录中。
4. 构建候选镜像 `cheedonghu/invest:release-<版本号>`。依赖未变时可基于原镜像替换应用源码；依赖有变化时使用项目 Dockerfile 完整构建。
5. 候选容器只绑定服务器回环地址，使用现有配置验证三处页面和数据接口，再切换线上镜像。
6. 在部署目录执行 `docker compose up -d --no-deps --pull never invest-app`，验证外网访问；失败时恢复旧镜像。

源码包不得包含 `.git`、私钥、本机 `config.toml`、本地测试输出或缓存。

容器启动脚本必须使用 LF 换行，仓库通过 `.gitattributes` 固定 `.sh` 的换行格式，避免 Windows 打包后容器无法执行入口脚本。

## 回滚

将 `<版本号>` 替换为需要回退的部署记录版本号。在服务器执行：

```sh
cd /usr/local/etc/invest
docker tag cheedonghu/invest:rollback-<版本号> cheedonghu/invest:latest
docker compose up -d --no-deps --pull never invest-app
```

回滚应用镜像不会恢复或清空数据库。
