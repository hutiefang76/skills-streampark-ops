---
name: streampark-ops
description: Apache StreamPark Flink 作业平台 API 运维 — 登录/查询应用/创建批任务/构建/启动/k8s 日志拉取，支持多环境 (uat/prod)
allowed-tools: Bash, Read, Write, Edit, Grep, Glob
---

# StreamPark Ops Skill

封装 Apache StreamPark 的 REST API，供 AI / 人类一句话操控 Flink 作业。

## Step 0: 解析 skill 根

```bash
# 优先固定路径
ls /d/workspace/skills/skills-streampark-ops/lib/sp_client.py 2>/dev/null && echo "FOUND:/d/workspace/skills/skills-streampark-ops"
# 兜底
find ~ /d/workspace -name "sp_client.py" -path "*streampark*" -maxdepth 7 2>/dev/null | head -1 | xargs -I{} dirname {} | xargs -I{} dirname {}
```

## Step 1: Pre-flight

```bash
ls <skill_root>/config.ini || echo "需先 copy config.ini.example → config.ini 并填密码"
<skill_root>/.venv/Scripts/python.exe -c "import requests" || echo "需先跑 setup.bat"
```

| 缺失 | 提示 |
|------|------|
| `config.ini` | 复制 `config.ini.example` → `config.ini`，填 StreamPark 地址/账号 |
| `.venv` 或 `requests` | 跑 `setup.bat` |

## Step 2: 可用命令

| 命令 | 用途 | 是否破坏性 |
|------|------|----------|
| `scripts/sp_apps_list.py --env <env>` | 列出该环境所有 Flink 应用 | ❌ 只读 |
| `scripts/sp_app_show.py --env <env> --name <jobName>` | 显示单应用详情 | ❌ 只读 |
| `scripts/sp_deploy_batch.py --env <env> --start YYYYMMDD --end YYYYMMDD` | 创建 MongoDB→Doris 批量回灌应用 | ✅ 写 |
| `scripts/sp_batch_ui/app.py` | 启动 Web Dashboard（端口 18792） | ✅ 写 |

## Step 3: 调用示例

```bash
cd <skill_root>
.venv/Scripts/python.exe scripts/sp_apps_list.py --env uat
.venv/Scripts/python.exe scripts/sp_deploy_batch.py --env uat --start 20260501 --end 20260507 --dry-run
```

## 环境映射

`config.ini` 中的 section `[env:uat]` `[env:prod]` 各自独立 host / user / pass。

## API 参考

| API | path | 用途 |
|-----|------|------|
| 登录 | `POST /passport/signin` | form: username/password/loginType=PASSWORD → 返回 token + userId + lastTeamId |
| 应用列表 | `POST /flink/app/list` | form: teamId/pageNum/pageSize/jobName |
| 创建应用 | `POST /flink/app/create` | form: 见 sp_deploy_batch.py payload |
| K8s 日志 | `POST /flink/app/k8sStartLog` | form: id |

所有请求 `Authorization: <token>` header。

## 注意事项

- StreamPark 使用 **form-urlencoded**，非 JSON
- token 短时有效，长任务每次 login
- `s.trust_env = False` / `os.environ.pop('HTTP_PROXY')` — 内网部署必须绕过系统代理
- 作业 `executionMode=6` = kubernetes-application
