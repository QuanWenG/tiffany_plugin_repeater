# Tiffany 群友学习型复读机

基于 Tiffany Hook、Service、Scope 编写的原生插件。功能与交互参考 `astrbot_plugin_repeater`，使用独立的消息模型、SQLite 结构和任务生命周期。

支持 Python 3.11+、提供本地源码插件 API 1 的 Tiffany 0.1.x；协议覆盖 OneBot v11 / NapCat 和 QQ 官方群聊。运行依赖为 Tiffany、aiohttp 和 jieba。

## 功能

- 学习同群相邻有效消息的关键词关系，按学习次数和近期话题选择接话。
- 连续相同消息达到阈值时跟随复读；纯图片和表情作为触发消息时只参与连续复读，文字前文可以检索图片或表情回答。
- 保存文字、图片、GIF 和 OneBot 原生 Face；图片按内容哈希缓存。默认开启文字和表情，普通图片需管理员开启。
- 清理结构化 @、图片附件描述和表情占位文本；命令、无法归属的引用、链接、语音、视频、文件、超限或无效内容切断学习关联。明确引用人类消息用于定向学习，引用机器人回答只记录互动。
- 以“龟龟”开头的正文获得点名检索行为；过滤反向学到的短称呼，较长回答发送时去掉句首称呼。
- 每群保存开关、分类开关和频率预设，支持喝酒、醒酒、顶人和两分钟内撤回复读。
- 沉默后主动发言，醉酒到期自动提醒；延迟回复交给 Runtime 监督的任务，发送前复查开关、封禁和额度。
- 可选的本机只读语料观察台：概览、文本与关键词查询、群/作者/类型/时间筛选、来源、关系和发送记录。
- 可追溯的人类聊天证据、一至三阶上下文回退，以及按共同回答建立的一跳表达邻居。
- 接收 NapCat 贴表情与明确引用回复，按不同参与者积累接话经验；引用吐槽同样计为互动。

0.3.0 的智能策略默认 `auto`：达到独立证据门槛后参与选句，没有新证据时继续使用原检索。详细行为、配置、迁移和验证方式见 [智能学习说明](INTELLIGENCE.md)。

当前版本 0.3.1 新增 @ 定向指令、按账号和群的 30 分钟静音，以及首次启动自动生成带注释的配置。服务器更新步骤见 [部署说明](DEPLOYMENT.md)。

发布说明见 [CHANGELOG.md](CHANGELOG.md)，测试与规模验收结果见 [IMPLEMENTATION_REVIEW.md](IMPLEMENTATION_REVIEW.md)。

## 安装和启动

本插件通过 Tiffany 的 `tiffany.plugins` entry point 注册，插件 ID 为 `repeater`，API 版本为 1。Tiffany 启动脚本负责 editable 安装、依赖检查、配置读取和初始化。

### Linux 服务器：/usr/local/Tiffany

停止正在运行的 Tiffany，将本项目完整源码目录放到：

```text
/usr/local/Tiffany/data/plugins/repeater/
├── pyproject.toml
├── README.md
├── LICENSE
├── examples/
└── tiffany_plugin_repeater/
    ├── __init__.py
    └── ...
```

也可以将发布的 `tiffany-plugin-repeater-0.3.1-source.zip` 解压到 `/usr/local/Tiffany/data/plugins/`，归档内已包含 `repeater/` 顶层目录。宿主按一级子目录中的 `pyproject.toml` 发现插件，保留这个项目文件。

继续使用 Tiffany 原来的脚本：

```bash
cd /usr/local/Tiffany
bash start.sh plugins list
bash start.sh plugins sync
bash start.sh plugins check
bash start.sh
```

`list` 应显示 `repeater` 为 `enabled`；`sync` 准备共享依赖环境；`check` 校验入口、配置和 Service 注册，校验过程不启动资源或连接平台。首次准备需要访问包索引，准备成功后的普通启动复用环境。普通启动也会自动同步新的有效插件。

首次正式启动会在 `/usr/local/Tiffany/data/config/repeater.toml` 自动生成完整配置，包含中文注释；已有文件不会覆盖，也不会自动补写缺失项。修改后重启生效。也可以在首次启动前手动复制项目内的 `examples/repeater.toml`。`plugins check` 只校验，不生成配置。自动生成失败时会记录警告，并继续使用已经加载的配置或默认值。

如果启动脚本使用了 `--home` 或 `TIFFANY_HOME`，将上述 `data` 换成对应 home；管理命令也使用相同的 `--home`。

### Windows 或独立源码目录

可以把整个项目放到 Tiffany 的 `data/plugins/repeater/`；也可以在 `data/config/plugins.toml` 的 `paths` 中登记源码目录，保留已有其他插件路径：

```toml
paths = ["D:/Projects/Python/tiffany_plugin_repeater"]
disabled = []
```

随后在 Tiffany 项目目录执行：

```powershell
.\start.bat plugins sync
.\start.bat plugins check
.\start.bat
```

两种发现方式选择一种即可；同步由框架完成。平台、凭据、调度器、日志和监控继续使用原来的 Tiffany 配置。

### 从 0.2.x 更新到 0.3.0

停止 Tiffany 后替换插件源码，再按原来的启动脚本启动。启动时校验数据库版本 1，使用 SQLite 备份接口生成 `repeater.v1-backup-<随机标识>.sqlite3`，随后在事务内升级为版本 2。原语料、群开关和频率保留，旧关系继续用于原检索；新证据从升级后开始积累。备份包含迁移前已经提交的 WAL 数据。

需要回退选句策略时使用 `/repeater intelligence off`。需要运行 0.2.x 程序时，停止 Tiffany 并恢复迁移前备份；旧程序不能读取版本 2 的数据库。

### 0.2.1 的历史修复

停止 Tiffany 后替换插件源码，再按原来的启动脚本启动。0.2.1 修复跨群答案次数、话题权重、遗忘后的学习次数、多群主动发言、表情元数据、占位文字清理和发送记录类型筛选；回归用例已加入默认测试。已有语料库在启动时会重算关系次数和最近出现时间，修正旧版遗忘操作遗留的计数。配置和数据库版本保持兼容。

### 从 0.1.0 更新

停止 Tiffany 后替换插件源码。如果之前按照旧安装说明在 `application.py` 中手动添加了复读机的 import、配置读取和 `install(...)` 注册，请删除那一段手动注册，使用新版框架自带的 `install_plugins(...)` 加载，避免重复注册。

0.2.0 由框架启动脚本运行，独立的 `tiffany-repeater` / `python -m tiffany_plugin_repeater` 启动器已经移除。原有 `data/config/repeater.toml`、`data/storage/repeater/repeater.sqlite3` 和媒体缓存继续使用，数据库结构保持版本 1。

### 启停和更新

```bash
bash /usr/local/Tiffany/start.sh plugins disable repeater
bash /usr/local/Tiffany/start.sh plugins enable repeater
```

管理修改和依赖同步需在 Tiffany 停止后执行。配置或普通源码变更后重启生效；声明、依赖或启用状态变更由启动器同步。只安装 wheel 不能代替本地源码插件发现；服务器部署请使用完整源码项目或源码 ZIP。

## 在自己的 Tiffany 应用中安装

标准入口为同步 `install(scope, config) -> None`，宿主传入 `plugins.repeater` Scope，以及 `config/repeater.toml` 的字典；配置不存在时传入空字典。安装函数验证配置并注册 Service 和群消息 Hook，数据库、HTTP 会话和任务在 Tiffany 生命周期中初始化。缺少配置文件时，在 Service setup 阶段根据本次启动配置生成带注释的文件，路径取自 `APP_PATHS`，不会写入当前工作目录。

```python
from core import APP_PATHS, Bot
from shared.paths import RuntimePaths
from tiffany_plugin_repeater import install

bot = Bot()
paths = RuntimePaths.resolve("./data")
bot.service(APP_PATHS, paths)
install(bot.scope("plugins.repeater"), {})
# 使用 bot.install(...) 安装 Tiffany 的 OneBot 或 QQ 官方 Adapter。
# 随后 await bot.run_async()。
```

自定义应用同时使用 `hooks.install_extensions` 时，先安装内置扩展，再安装本插件，以便复用已注册的命令和管理员权限。普通服务器启动使用框架自动发现即可。

`SERVICE` 是原生 ServiceKey，可使用 `bot.services.get(SERVICE)` 查看服务。卸载用 `await bot.scope("plugins.repeater").unload(mode="abort")`；正常停止也会取消延迟回复和醒酒提醒、结算存储操作并关闭资源。

## 群聊命令

所有成员可使用下列命令，也支持前置 `/`、`/` 后空格和结构化 @机器人：

多机器人同群时，使用平台的 @ 功能选中目标账号，例如 `@机器人A /repeater frequency 活跃的`、`@机器人B 龟龟闭嘴`。本插件的所有指令只由被 @ 的账号执行，其他账号不会执行或学习这条指令；同时 @ 多个账号时，这些账号分别执行。`@全体成员` 不作为对所有机器人的指令。未 @ 的指令继续按原行为处理，多个机器人可能同时响应。

| 命令 | 行为 |
| --- | --- |
| 龟龟喝酒 / 龟龟干杯 / 龟龟继续喝 | 本群临时放宽学习、接话与频率限制，默认五分钟，再次使用重新计时 |
| 龟龟醒酒 / 龟龟醒一醒 / 龟龟别喝了 | 立即恢复本群预设，取消到期提醒 |
| 龟龟顶人 | 根据近期语料立即接话；没有合适回答时从八句固定回话中随机选择 |
| 龟龟缩头 | 固定道歉，并撤回 120 秒内最近一条具有成功回执的复读 |
| 龟龟闭嘴 | 本账号在当前群静音，默认 30 分钟；重复使用重新计时 |
| 龟龟说话 | 提前解除本账号在当前群的静音，不改变群开关和频率 |

静音期间继续学习，暂停普通接话、主动发言、顶人、缩头、喝酒/醒酒互动及自动醒酒提醒；管理指令仍可使用，`/repeater status` 显示静音剩余时间。到期自动恢复，不发送额外通知。静音只控制本插件，不影响其他插件。`mute_duration_seconds = 1800` 可修改默认时长；已有配置不需要补写此项即可使用默认值。静音状态与喝酒状态一样不跨重启保存。

`drink_admin_only = true` 可限制喝酒/醒酒权限。喝酒期间放宽单字学习、学习次数门槛、连续复读阈值和冷却，原来的去重、封禁与分类开关继续生效。

管理员命令：

```text
/repeater status
/repeater frequency
/repeater frequency 沉默的
/repeater frequency 活跃的
/repeater frequency 话痨的
/repeater intelligence
/repeater intelligence auto
/repeater intelligence observe
/repeater intelligence off
/repeater on
/repeater off
/repeater text on
/repeater expression off
/repeater media on
/repeater forget
/repeater forget_keyword
/repeater clear confirm
```

管理员来自本插件 `admin_ids`、OneBot 群主/管理员角色（`allow_group_admin` 控制），或已安装的 Tiffany permissions 扩展。QQ 官方群聊使用不透明 OpenID，不转换为 QQ 号。`admin_ids` 与 `ignored_user_ids` 中的 ID 在此插件配置覆盖的所有账户生效；需要区分 Adapter 的管理员请使用 Tiffany permissions 配置。

`forget` 删除并在本群封禁最近一次复读的完整内容；`forget_keyword` 保留语料，封禁对应回答关键词。`clear confirm` 清除本群语料、关系、发送历史与禁用记录，保留开关和频率。关闭分类不会删掉已有语料；混合消息要求其全部分类开启。

频率以配置为基准：沉默的保持原参数；活跃的将等待缩为 1/2、额度乘 2，并最多要求学习两次；话痨的缩为 1/4、额度乘 4，学习一次即可成为候选，连续复读阈值最多为两次。醉酒效果叠加在当前预设上。概率、学习次数、去重和平台限制仍会影响回复。

## 数据与观察台

所有内容位于 `<home>/storage/repeater/`：

```text
repeater.sqlite3
media/<sha256>.<格式>
```

SQLite 使用 WAL、FULL 同步和专属单线程写执行器；选句、观察台和图计算使用有界的独立读取或后台执行资源。调用被取消后，已经提交到线程的存储操作仍会完成；关闭前会等待这些操作结算。新建数据库版本为 2，版本 1 自动备份迁移；未知版本或不匹配的表结构在启动时失败并保留原文件。

群与事件身份包含 Adapter 和机器人账户；纯文本只有在同一账户下至少两个不同群学习后才可跨群复用（可配置），图片和表情始终限来源群。保存作者 ID 和平台消息 ID，不保存群昵称。原始消息每群最多保留 10,000 条，发送历史至少保留最近 100 条。低频陈旧关系和事件去重记录定期清理，无引用的媒体在维护时回收，并给正在处理的文件保留至少一分钟宽限。

启用 `dashboard_enabled` 并设置至少 16 字符的 `dashboard_token` 后，浏览器打开 `http://127.0.0.1:9470`，输入令牌连接。API 仅提供 GET 查询，使用 `Authorization: Bearer <令牌>`；令牌保存在当前页面内存中。观察台只允许监听本机，远程访问可通过 SSH 端口转发。

统计覆盖的是当前仍保留的数据。关系没有逐次关联原始消息，来源详情显示现存同内容记录，不保证是最初出处或某次回复的唯一来源。

## 平台与验证边界

OneBot 发送图片使用 base64 消息段，适用于 NapCat 与 Tiffany 位于不同主机。只撤回本服务发送时取得的消息 ID，命令提示和自动醒酒通知不进入撤回候选。重连后使用该账户后续事件携带的新 Client 更新发送路由；断线期间发送会失败并释放预留。

QQ 官方直接使用 Tiffany 的 `reply_message`、`send_message`、`upload_file` 和 `delete_message`。文本及图片群消息、OpenID 权限和撤回具有自动测试；Face 仅用于 OneBot。多张图片分条发送，已成功的部分立即记回执，后续失败也不会把整次操作当成完全未发送。QQ 的全量群消息、主动发言、上传和撤回能否使用，需要实际账号权限及平台响应确认。

主动发言要求本次运行至少学习十条有效消息、已有复读发言且之后仍有群聊活动。运行中的醉酒、活动计数和撤回回执不跨重启保存。

## 开发测试

安装开发环境依赖后运行：

```powershell
python -m unittest discover -s tests -v
```

宿主 API 测试需要 Tiffany 源码可导入。在 Windows 上先设置 `$env:PYTHONPATH = (Resolve-Path ../Tiffany).Path`；Linux 可用 `PYTHONPATH=/path/to/Tiffany python -m unittest discover -s tests -v`。运行解释器需要已经安装本项目及声明的运行依赖。

测试使用真实 Tiffany Runtime、Provider、Scope、Service、插件发现/安装器，以及本地 OneBot WebSocket 和 QQ Client HTTP 往返。涵盖 API 1 声明、配置注入、禁用、注册与初始化回滚、资源 owner、上下文、跨群边界、多模态、权限、冷却预留、失败回滚、部分发送、重连、观察台鉴权和卸载清理。
