# sp_batch_ui — StreamPark MongoDB→Doris 批量回灌 Web Dashboard

单文件 stdlib http.server 应用（端口 18792），可视化挑应用、调阈值、看 JM 日志、对比 Mongo↔Doris 行数差。

## 启动

```cmd
:: 在 skills-streampark-ops 根目录
.venv\Scripts\python.exe scripts\sp_batch_ui\app.py
```

浏览器打开 http://localhost:18792

## 配置来源

| 配置项 | 来源 |
|--------|------|
| StreamPark 凭据 (base/user/password) | `skills-streampark-ops/config.ini` 的 `[env:uat]` |
| 切换环境 | 设置 `SP_ENV=prod` 环境变量 |
| MongoDB / Doris 凭据 + 批配置 | `sp_batch_ui/config.json`（首次启动自动从 `config.json.example` 生成）|

## 安全

- `config.json` 含 Mongo/Doris 真实凭据，已被 `.gitignore` 排除
- `data/` 目录（运行时状态/PID/log）也被排除
- 修改后凭据不入 git

## 关闭

```cmd
:: 找端口对应 PID
netstat -ano | findstr :18792
:: 杀进程
taskkill /F /PID <pid>
```

或直接停 cmd 窗口。
