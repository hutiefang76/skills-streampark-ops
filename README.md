# skills-streampark-ops

Apache StreamPark REST API 运维 skill — Flink 作业平台一句话操控,跨平台 (macOS/Linux/Windows)。

## 能力

- 查询: 列出所有 Flink 应用、单应用详情、状态/构建/日志
- 部署: 创建批任务 (示例脚本, 所有参数走 placeholder, 必须 `--jar / --main-class / --flink-image / --nacos-addr` 显式覆盖)
- Dashboard: 单文件 Web UI (端口 18792), 可视化挑选 / 重跑 / 阈值调节

## 装

```bash
# 通过 frank (推荐)
frank install streampark-ops

# 或直接 git clone
git clone https://github.com/hutiefang76/skills-streampark-ops.git ~/.claude/skills/streampark-ops
```

## 用

```bash
cd ~/.claude/skills/streampark-ops

# Mac/Linux
bash setup.sh
# Windows
# setup.bat

cp config.ini.example config.ini
# 编辑 config.ini 填 base/user/password

.venv/bin/python scripts/sp_apps_list.py --env local       # Mac/Linux
# .venv\Scripts\python.exe scripts\sp_apps_list.py --env local  # Windows
```

完整命令清单见 `SKILL.md`。

## Demo

```bash
# 本机起一个 StreamPark (用 frank 仓库的 demo stack):
git clone https://github.com/hutiefang76/skills-frank.git
cd skills-frank/deploy/test-stack
docker compose up -d streampark
# 浏览器开 http://localhost:10000, 账号 admin/streampark
```

然后 `config.ini` 用 `localhost:10000 / admin / streampark`, 直接 `sp_apps_list.py --env local` 就能看到 demo 集群的应用。

## 配置

`config.ini` 按环境分 section:

```ini
[env:local]
base     = http://localhost:10000
user     = admin
password = streampark

[env:uat]
base     = http://your-streampark-uat:10000
user     = admin
password = your-password

[http]
trust_env = false   ; 内网部署 strip 系统 proxy; 跨外网 demo 改 true
```

## 文件结构

```
skills-streampark-ops/
├── SKILL.md                  AI 加载入口 (YAML frontmatter + 工作流)
├── README.md                 本文件
├── manifest.yaml             依赖声明 (接入用户私有 manifest 时使用)
├── requirements.txt          Python 依赖 (requests / urllib 内置)
├── config.ini.example        配置模板
├── setup.sh                  Mac/Linux 一键装
├── setup.bat                 Windows 一键装
├── lib/
│   └── sp_client.py          StreamPark HTTP 客户端 (trust_env 可控)
└── scripts/
    ├── sp_apps_list.py       列应用 (只读)
    ├── sp_app_show.py        查应用 (只读)
    ├── sp_deploy_batch.py    创建批应用 (示例, 所有 image/jar/main-class/nacos-addr placeholder, 必须显式传值)
    └── sp_batch_ui/          Web Dashboard (端口 18792)
```

## 平台支持

| OS | setup | 备注 |
|----|-------|------|
| macOS | `bash setup.sh` | 需 python 3.8+ |
| Linux | `bash setup.sh` | 同上 |
| Windows | `setup.bat` | python 3.8+, venv 在 `.venv\Scripts\` |

## 凭据安全

- `config.ini` 由 `.gitignore` 排除
- `config.ini.example` 仅含占位符
- 真实凭据**绝不入库**

## License

MIT
