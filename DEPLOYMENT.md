# 0.3.1 部署说明

部署包：`tiffany-plugin-repeater-0.3.1-source.zip`。归档内自带 `repeater/` 顶层目录，包含完整源码、插件声明、配置示例、观察台页面和测试。

## Linux：/usr/local/Tiffany

1. 停止正在运行的 Tiffany。
2. 将 ZIP 上传到服务器，然后解压到 Tiffany 的 `data/plugins/`，覆盖原来的 `repeater/` 插件目录。已有 `data/config/repeater.toml` 和 `data/storage/repeater/` 继续使用。

```bash
cd /usr/local/Tiffany
unzip -o /path/to/tiffany-plugin-repeater-0.3.1-source.zip -d data/plugins/
bash start.sh plugins sync
bash start.sh plugins check
bash start.sh
```

将 `/path/to/` 改为 ZIP 实际上传位置。使用 `--home` 或 `TIFFANY_HOME` 的部署，插件、配置和管理命令均使用对应的 home。若通过 `plugins.toml` 的 `paths` 登记了独立源码目录，更新该目录中的文件。

首次正式启动时，缺少的 `config/repeater.toml` 会自动生成；已有文件保持原样。`plugins check` 只校验注册和配置。

## 更新后验证

通过群聊的 @ 功能选中目标账号，再输入命令。例如：

```text
@机器人A /repeater status
@机器人A /repeater frequency 活跃的
@机器人A 龟龟闭嘴
@机器人A 龟龟说话
```

仅被 @ 的机器人执行指令。未 @ 的指令保留原行为，多个机器人可能同时响应。

“龟龟闭嘴”只让该账号在当前群静音，默认 30 分钟；再次使用重新计时。“龟龟说话”提前解除静音，原本关闭的群仍保持关闭。静音期间继续学习，暂停复读、主动发言、顶人等互动；管理指令仍可用，`/repeater status` 显示剩余时间。静音仅作用于本插件，且不跨重启保存。

需要调整默认时长，可在已有 `repeater.toml` 中加入：

```toml
mute_duration_seconds = 1800
```

修改配置后重启生效。未配置此项时默认使用 1800 秒。

## 验证记录

本次补丁的功能与集成验证见 `audit/release-0.3.1.json`。`IMPLEMENTATION_REVIEW.md` 与 `audit/intelligence-scale-20261023.json` 记录的是 0.3.0 的历史规模验收，不代表本补丁重新执行了 30 分钟压力测试。
