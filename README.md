# skills-streampark-ops

Apache StreamPark REST API 运维 skill — Flink 作业平台一句话操控。

## 能力

- 查询：列出所有 Flink 应用、单应用详情、状态/构建/日志
- 部署：创建批任务（MongoDB→Doris 批量回灌为典型场景）
- Dashboard：单文件 Web UI（端口 18792），可视化挑选 / 重跑 / 阈值调节

## 安装

```cmd
git clone git@github.com:hutiefang76/skills-streampark-ops.git
cd skills-streampark-ops

copy config.ini.example config.ini
notepad config.ini      :: 填 base / user / password / team_id

setup.bat               :: 自动建 .venv (Python 3.12+) + 装依赖
```

## 验证

```cmd
.venv\Scripts\python.exe scripts\sp_apps_list.py --env uat
```

预期：返回该环境所有 Flink 应用 jobName + state 表格。

## 配置

`config.ini` 按环境分 section：

```ini
[env:uat]
base = http://10.0.0.121:31000
user = admin
password = streampark

[env:prod]
base = http://prod-streampark.example:31000
user = admin
password = ***
```

## 文件结构

```
skills-streampark-ops/
├── SKILL.md                  AI 加载入口（YAML frontmatter + 工作流）
├── README.md                 本文件
├── manifest.yaml             依赖声明 + config 渲染规则（接入 kdwl 时使用）
├── requirements.txt          Python 依赖
├── config.ini.example        配置模板
├── setup.bat                 一键安装
├── lib/
│   └── sp_client.py          StreamPark HTTP 客户端
└── scripts/
    ├── sp_apps_list.py       列应用（只读）
    ├── sp_app_show.py        查应用（只读）
    ├── sp_deploy_batch.py    创建批应用（写）
    └── sp_batch_ui/          Web Dashboard（端口 18792）
```

## 凭据安全

- `config.ini` 由 `.gitignore` 排除
- `config.ini.example` 仅含占位符
- 真实凭据**绝不入库**

## License

MIT
