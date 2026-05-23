"""StreamPark HTTP 客户端 — 登录 / 应用列表 / 创建 / 启动 / 日志。

封装 form-urlencoded + token header + 代理绕过的统一调用入口。
"""
from __future__ import annotations
import os
import json
import configparser
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Optional


class SPError(RuntimeError):
    pass


class SPClient:
    """单环境单例 StreamPark 客户端。

    用法:
        sp = SPClient.from_config('uat')
        sp.login()
        apps = sp.list_apps()
    """

    def __init__(self, base: str, user: str, password: str, timeout: int = 30,
                 trust_env: bool = False):
        self.base = base.rstrip('/')
        self.user = user
        self.password = password
        self.timeout = timeout
        self.token: Optional[str] = None
        self.user_id: Optional[int] = None
        self.team_id: Optional[int] = None
        # 默认 strip 所有 proxy env (StreamPark 内网部署最常见情况).
        # trust_env=True 时保留 (跨外网 demo, 或 proxy 必经路径).
        if not trust_env:
            for k in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy',
                      'ALL_PROXY', 'all_proxy'):
                os.environ.pop(k, None)

    @classmethod
    def from_config(cls, env: str, config_path: Optional[str] = None) -> 'SPClient':
        if config_path is None:
            here = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.normpath(os.path.join(here, '..', 'config.ini'))
        if not os.path.exists(config_path):
            raise SPError(f"config.ini not found: {config_path}. "
                          f"Copy config.ini.example and fill credentials.")
        cfg = configparser.ConfigParser()
        cfg.read(config_path, encoding='utf-8')
        section = f'env:{env}'
        if section not in cfg:
            raise SPError(f"env [{section}] missing in {config_path}")
        timeout = int(cfg.get('http', 'timeout', fallback='30')) if cfg.has_section('http') else 30
        trust_env_raw = cfg.get('http', 'trust_env', fallback='false') if cfg.has_section('http') else 'false'
        trust_env = trust_env_raw.strip().lower() in ('true', 'yes', '1', 'on')
        return cls(
            base=cfg[section]['base'],
            user=cfg[section]['user'],
            password=cfg[section]['password'],
            timeout=timeout,
            trust_env=trust_env,
        )

    def _http(self, method: str, path: str, headers: Optional[dict] = None,
              form: Optional[dict] = None, raw: Optional[bytes] = None) -> dict:
        url = self.base + path
        h = dict(headers or {})
        body = None
        if form is not None:
            body = urllib.parse.urlencode(form).encode('utf-8')
            h.setdefault('Content-Type', 'application/x-www-form-urlencoded')
        elif raw is not None:
            body = raw
        req = urllib.request.Request(url, data=body, method=method, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return json.loads(r.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            return {'code': e.code, 'msg': (e.read() or b'').decode('utf-8', 'replace')}

    def login(self) -> str:
        r = self._http('POST', '/passport/signin',
                       form={'username': self.user, 'password': self.password,
                             'loginType': 'PASSWORD'})
        data = r.get('data') or {}
        if not data.get('token'):
            raise SPError(f"login failed: {r}")
        self.token = data['token']
        self.user_id = data['user']['userId']
        self.team_id = data['user']['lastTeamId']
        return self.token

    def _auth(self) -> dict:
        if not self.token:
            self.login()
        return {'Authorization': self.token}

    def list_apps(self, job_name: str = '', page_num: int = 1, page_size: int = 100) -> list[dict]:
        r = self._http('POST', '/flink/app/list',
                       headers=self._auth(),
                       form={'teamId': self.team_id, 'pageNum': page_num,
                             'pageSize': page_size, 'jobName': job_name})
        return (r.get('data') or {}).get('records', [])

    def find_app(self, job_name: str) -> Optional[dict]:
        for a in self.list_apps(job_name=job_name):
            if a.get('jobName') == job_name:
                return a
        return None

    def create_app(self, payload: dict) -> dict:
        payload.setdefault('teamId', self.team_id)
        payload.setdefault('userId', self.user_id)
        return self._http('POST', '/flink/app/create',
                          headers=self._auth(), form=payload)

    def release_app(self, app_id: int) -> dict:
        return self._http('POST', '/flink/app/release',
                          headers=self._auth(), form={'id': app_id})

    def start_app(self, app_id: int) -> dict:
        return self._http('POST', '/flink/app/start',
                          headers=self._auth(),
                          form={'id': app_id, 'savePointed': 'false',
                                'flameGraph': 'false', 'allowNonRestored': 'false'})

    def k8s_start_log(self, app_id: int, offset: int = 0, limit: int = 200) -> dict:
        return self._http('POST', '/flink/app/k8sStartLog',
                          headers=self._auth(),
                          form={'id': app_id, 'offset': offset, 'limit': limit})


if __name__ == '__main__':
    import sys
    env = sys.argv[1] if len(sys.argv) > 1 else 'uat'
    sp = SPClient.from_config(env)
    sp.login()
    print(f"[OK] logged in env={env} user_id={sp.user_id} team_id={sp.team_id}")
    apps = sp.list_apps()
    print(f"[OK] {len(apps)} apps found")
