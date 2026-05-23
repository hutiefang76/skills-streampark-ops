---
name: streampark-ops
description: 'Apache StreamPark Flink 作业平台 API 运维 — 登录/查询应用/显示详情/创建批任务/日志拉取, 多环境 (local/uat/prod) 支持'
allowed-tools: Bash, Read, Write, Edit, Grep, Glob
---

# StreamPark Ops Skill

封装 Apache StreamPark 的 REST API, 给 AI / 人类一句话操控 Flink 作业。

## Step 0: 定位 skill 根

```bash
# frank install 装的会软链到三平台 skills/
SKILL_ROOT=$(ls -d ~/.claude/skills/streampark-ops 2>/dev/null \
          || ls -d ~/.codex/skills/streampark-ops 2>/dev/null \
          || ls -d ~/.opencode/skills/streampark-ops 2>/dev/null)
echo "SKILL_ROOT=$SKILL_ROOT"
```

## Step 1: Pre-flight

```bash
test -f "$SKILL_ROOT/config.ini" || echo "需先 cp config.ini.example config.ini 并填地址/账号"
test -d "$SKILL_ROOT/.venv" || echo "需先跑 bash setup.sh (或 setup.bat)"
```

| 缺失 | 修复 |
|------|------|
| `config.ini` | `cp config.ini.example config.ini` 然后填 base/user/password |
| `.venv` | `bash setup.sh` (Mac/Linux) 或 `setup.bat` (Windows) |

## Step 2: 可用命令

| 命令 | 用途 | 破坏性 |
|------|------|--------|
| `scripts/sp_apps_list.py --env <env>` | 列该环境所有 Flink 应用 | ❌ 只读 |
| `scripts/sp_app_show.py --env <env> --name <jobName>` | 单应用详情 | ❌ 只读 |
| `scripts/sp_deploy_batch.py --env <env> --start <YYYYMMDD> --end <YYYYMMDD>` | 创建批任务 (默认值需按业务改) | ✅ 写 |
| `scripts/sp_batch_ui/app.py` | Web Dashboard (端口 18792) | ✅ 写 |

## Step 3: 调用示例

```bash
cd "$SKILL_ROOT"
.venv/bin/python scripts/sp_apps_list.py --env local
.venv/bin/python scripts/sp_app_show.py --env local --name <jobName>
.venv/bin/python scripts/sp_deploy_batch.py --env local --start 20260501 --end 20260507 --dry-run
```

Windows 用户:
```cmd
.venv\Scripts\python.exe scripts\sp_apps_list.py --env local
```

## 环境配置

`config.ini` 中每个环境一个 `[env:<name>]` section:
```ini
[env:local]
base     = http://localhost:10000
user     = admin
password = streampark

[env:uat]
base     = http://your-streampark-uat:10000
user     = admin
password = your-password
```

## Demo 环境 (Docker)

`frank` 仓库 `deploy/test-stack/docker-compose.yml` 一键起本机 StreamPark:
```bash
docker compose -f deploy/test-stack/docker-compose.yml up -d streampark
# StreamPark 默认 http://localhost:10000, 账号 admin/streampark
```

## API 参考

| API | path | 用途 |
|-----|------|------|
| 登录 | `POST /passport/signin` | form: `username/password/loginType=PASSWORD` → 返回 `token + userId + lastTeamId` |
| 应用列表 | `POST /flink/app/list` | form: `teamId/pageNum/pageSize/jobName` |
| 单应用 | `POST /flink/app/list` (jobName=精确匹配) | 同上 |
| 创建 | `POST /flink/app/create` | form: 见 `sp_deploy_batch.py` payload |
| 启动 | `POST /flink/app/start` | form: `id` |
| K8s 日志 | `POST /flink/app/k8sStartLog` | form: `id` |

所有请求 `Authorization: <token>` header。

## 注意事项

- StreamPark 使用 **form-urlencoded**, 非 JSON
- Token 有效期短, 长任务每次 login
- **内网部署需绕过系统代理** — `sp_client.py` 默认 strip 所有 `HTTP_PROXY` env; 跨外网 demo 时可在 config.ini 加 `[http] trust_env = true` 关掉 strip
- 作业 `executionMode=6` = kubernetes-application
- `sp_deploy_batch.py` 默认值 (Flink 镜像/Nacos 地址/Jar 名) 基于内部业务, **demo 用必须 --jar / --main-class / --flink-image 覆盖**
