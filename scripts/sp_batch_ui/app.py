"""
Streampark MongoDB→Doris 批量回灌 Web Dashboard (单文件 app)

布局（所有产物本目录隔离）:
  tools/sp_batch_ui/
    ├── app.py             # 本文件
    ├── config.json        # SP app id / Mongo / Doris 配置（首次启动自动生成）
    ├── data/
    │   ├── batch_state.json    # 每日 mongo/doris/cov 持久化
    │   ├── app.pid             # 进程锁
    │   └── app.log             # 实时日志
    └── 启动.bat / 启动.ps1

特性:
  - 端口 18792 (避开 SP 31000 / Flink 8081-8083 / OpenClaw 18789 / Edict 7891)
  - 任务选择: 从 SP /flink/app/list 拉出所有 batch app 让用户挑
  - SP 参数面板: 实时显示选中 app 的 args / options / dynProps / state
  - JM 日志面板: 拉 k8sStartLog 实时展示
  - 数据清空: SQL 模板 + 一键执行
  - 重跑任意日期 / 全部重跑 / 阈值调节
  - 双开保护 (PID + 进程存活检测)

启动: python app.py
"""
import os
import sys
import json
import time
import threading
import socketserver
import urllib.parse
import urllib.request
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler

for k in ['HTTP_PROXY','HTTPS_PROXY','http_proxy','https_proxy','ALL_PROXY','all_proxy']:
    os.environ.pop(k, None)

# ============ 目录隔离 ============
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)
CONFIG_FILE = os.path.join(APP_DIR, 'config.json')
STATE_FILE = os.path.join(DATA_DIR, 'batch_state.json')
PID_FILE = os.path.join(DATA_DIR, 'app.pid')
LOG_FILE = os.path.join(DATA_DIR, 'app.log')

# 凭据从 skill 根 config.ini 加载（多环境支持），其余配置在 sp_batch_ui/config.json
# 启动时若 config.json 不存在，从 config.json.example 模板生成
import configparser

SKILL_ROOT = os.path.abspath(os.path.join(APP_DIR, '..', '..'))
SKILL_CONFIG = os.path.join(SKILL_ROOT, 'config.ini')
CONFIG_EXAMPLE = os.path.join(APP_DIR, 'config.json.example')

DEFAULT_CONFIG = {
    "host": "0.0.0.0",
    "port": 18792,
    "env": "uat",
    "streampark": {
        "base": "",
        "user": "",
        "password": "",
        "default_app_id": "",
    },
    "mongo": {
        "uri": "REPLACE_ME_MONGO_URI",
        "db": "magnolia_prod",
        "collection_pattern": "event_data_{ymd}",
    },
    "doris": {
        "host": "10.0.0.203",
        "port": 9030,
        "user": "REPLACE_ME",
        "password": "REPLACE_ME",
        "table": "uat_prod_ods_mongo_magnolia.event_data",
        "date_column": "shard_date",
        "partition_pattern": "p{ymd}",
    },
    "window_timeout_seconds": 5400,
}


def _load_streampark_from_skill():
    """从 skill 根 config.ini 加载 StreamPark 凭据（--env 决定 section）。"""
    env = os.environ.get('SP_ENV') or DEFAULT_CONFIG['env']
    if not os.path.exists(SKILL_CONFIG):
        return None
    cp = configparser.ConfigParser()
    cp.read(SKILL_CONFIG, encoding='utf-8')
    section = f'env:{env}'
    if section not in cp:
        return None
    return {
        'base': cp[section].get('base', ''),
        'user': cp[section].get('user', ''),
        'password': cp[section].get('password', ''),
    }


def load_config():
    if not os.path.exists(CONFIG_FILE):
        if os.path.exists(CONFIG_EXAMPLE):
            with open(CONFIG_EXAMPLE, encoding='utf-8') as f:
                seed = json.load(f)
        else:
            seed = DEFAULT_CONFIG
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(seed, f, indent=2, ensure_ascii=False)
        cfg = dict(seed)
    else:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
    # 补齐缺省 key
    for k, v in DEFAULT_CONFIG.items():
        cfg.setdefault(k, v)
        if isinstance(v, dict):
            for k2, v2 in v.items():
                cfg[k].setdefault(k2, v2)
    # 用 skill 根 config.ini 覆盖 streampark 凭据
    sp_from_skill = _load_streampark_from_skill()
    if sp_from_skill:
        cfg['streampark'].update({k: v for k, v in sp_from_skill.items() if v})
    return cfg


CFG = load_config()

# ============ 状态全局 ============
_worker_thread = None
_worker_stop = threading.Event()
_worker_status = {"running": False, "msg": "idle", "started_at": None,
                  "current_window": None, "windows_done": 0, "windows_total": 0,
                  "app_id": CFG['streampark']['default_app_id']}
_log_lock = threading.Lock()

# 后台 Doris 自动刷新
_auto_refresh = {
    'enabled': True,
    'interval': 30,
    'start': '20260401',
    'end': '20260430',
    'last_run': None,
    'last_doris_total': 0,
    'last_doris_total_prev': 0,
    'remaining': 1000,             # 配额: 用完自动停, 手动点击 + 1000
    'reload_on_click': 1000,
}


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    with _log_lock:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + "\n")
    print(line, flush=True)


def read_log_tail(n=300):
    if not os.path.exists(LOG_FILE):
        return ""
    with open(LOG_FILE, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    return "".join(lines[-n:])


# ============ HTTP / SP API ============
def http(m, p, h=None, f=None, timeout=30):
    body = urllib.parse.urlencode(f).encode() if f else None
    req = urllib.request.Request(CFG['streampark']['base'] + p, data=body, method=m, headers=h or {})
    if body:
        req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    handler = urllib.request.ProxyHandler({})
    opener = urllib.request.build_opener(handler)
    with opener.open(req, timeout=timeout) as r:
        return json.loads(r.read())


def sp_login():
    r = http('POST', '/passport/signin',
             f={'username': CFG['streampark']['user'],
                'password': CFG['streampark']['password'],
                'loginType': 'PASSWORD'})
    return r['data']['token'], r['data']['user']['lastTeamId']


def sp_list_batch_apps(tok, team_id):
    r = http('POST', '/flink/app/list',
             h={'Authorization': tok},
             f={'teamId': team_id, 'pageNum': 1, 'pageSize': 100})
    out = []
    for a in r.get('data', {}).get('records', []):
        name = a.get('jobName', '')
        # 只选含 "batch" 字样的；其余也列出但标记
        out.append({
            'id': str(a.get('id')),
            'jobName': name,
            'state': a.get('state'),
            'is_batch': 'batch' in name.lower(),
            'args': a.get('args', ''),
        })
    return out


def sp_app_get(tok, app_id):
    return http('POST', '/flink/app/get', h={'Authorization': tok}, f={'id': app_id})['data']


def sp_app_update_args(tok, app_id, new_args, parallelism=None, jm_mem=None, tm_mem=None):
    """更新 args 以及 options.parallelism / JM / TM 内存"""
    d = sp_app_get(tok, app_id)
    payload = {k: d.get(k, '') for k in
               ['id','teamId','jobName','jobType','executionMode','projectId','versionId',
                'module','jar','mainClass','options','dynamicProperties','flinkImage',
                'k8sNamespace','resolveOrder','k8sRestExposedType','hotParams','appType',
                'resourceFrom','description','tags']}
    payload['args'] = new_args
    if parallelism or jm_mem or tm_mem:
        try:
            opts = json.loads(d.get('options') or '{}')
        except Exception:
            opts = {}
        if parallelism: opts['parallelism.default'] = int(parallelism)
        if jm_mem:      opts['jobmanager.memory.process.size'] = jm_mem
        if tm_mem:      opts['taskmanager.memory.process.size'] = tm_mem
        payload['options'] = json.dumps(opts)
    payload.update({'restoreOrTriggerSavepoint':'false','allowNonRestored':'false',
                    'drain':'false','k8sHadoopIntegration':'false'})
    return http('POST','/flink/app/update', h={'Authorization': tok}, f=payload)


def sp_app_start(tok, app_id):
    return http('POST','/flink/app/start', h={'Authorization': tok},
                f={'id': app_id, 'savePointed':'false','allowNonRestored':'false','restoreMode':'0'})


def sp_app_cancel(tok, app_id):
    return http('POST','/flink/app/cancel', h={'Authorization': tok},
                f={'id': app_id, 'savePointed':'false','drain':'false','nativeFormat':'false'})


def sp_app_jmlog(tok, app_id, limit=10000):
    r = http('POST','/flink/app/k8sStartLog', h={'Authorization': tok},
             f={'id': app_id, 'offset': 0, 'limit': limit})
    return r.get('data', '') or ''


# ============ 数据探针 ============
def doris_counts(day_list):
    import mysql.connector
    c = mysql.connector.connect(host=CFG['doris']['host'], port=CFG['doris']['port'],
                                user=CFG['doris']['user'], password=CFG['doris']['password'])
    cur = c.cursor()
    start = min(day_list).isoformat()
    end = max(day_list).isoformat()
    cur.execute(f"""SELECT {CFG['doris']['date_column']}, COUNT(*)
                    FROM {CFG['doris']['table']}
                    WHERE {CFG['doris']['date_column']} BETWEEN '{start}' AND '{end}'
                    GROUP BY {CFG['doris']['date_column']}""")
    out = {str(d): r for d, r in cur.fetchall()}
    c.close()
    return out


def mongo_counts(day_list):
    from pymongo import MongoClient
    cli = MongoClient(CFG['mongo']['uri'], serverSelectionTimeoutMS=15000)
    db = cli[CFG['mongo']['db']]
    out = {}
    for d in day_list:
        coll = CFG['mongo']['collection_pattern'].format(ymd=d.strftime('%Y%m%d'))
        try:
            out[d.isoformat()] = db[coll].estimated_document_count()
        except Exception:
            out[d.isoformat()] = 0
    cli.close()
    return out


def doris_drop_partition(day_obj):
    import mysql.connector
    c = mysql.connector.connect(host=CFG['doris']['host'], port=CFG['doris']['port'],
                                user=CFG['doris']['user'], password=CFG['doris']['password'])
    cur = c.cursor()
    pname = CFG['doris']['partition_pattern'].format(ymd=day_obj.strftime('%Y%m%d'))
    next_day = day_obj + timedelta(days=1)
    sql_drop = f"ALTER TABLE {CFG['doris']['table']} DROP PARTITION IF EXISTS {pname}"
    sql_add = (f"ALTER TABLE {CFG['doris']['table']} ADD PARTITION {pname} "
               f"VALUES [('{day_obj.isoformat()}'), ('{next_day.isoformat()}'))")
    try:
        cur.execute(sql_drop)
        cur.execute(sql_add)
        return True, f"{sql_drop}; {sql_add}"
    except Exception as e:
        return False, str(e)
    finally:
        c.close()


# ============ 状态文件 ============
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_state(st):
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(st, f, indent=2, ensure_ascii=False)


def refresh_coverage(st, day_list):
    log(f"refresh coverage for {len(day_list)} days ...")
    doris = doris_counts(day_list)
    mongo = mongo_counts(day_list)
    for d in day_list:
        k = d.isoformat()
        m = mongo.get(k, 0)
        x = doris.get(k, 0)
        cov = round(x / m, 4) if m > 0 else 0.0
        st.setdefault(k, {})
        st[k].update({'mongo': m, 'doris': x, 'coverage': cov,
                      'checked_at': time.strftime('%Y-%m-%d %H:%M:%S')})
    return st


# ============ 后台 Doris 自动刷新 ============
def _auto_refresh_doris(start_d, end_d):
    """只 query Doris (跳过 Mongo)，更新 state 文件中 doris/coverage 字段"""
    days = []
    cur = start_d
    while cur <= end_d:
        days.append(cur); cur += timedelta(days=1)
    try:
        doris = doris_counts(days)
        st = load_state()
        for d in days:
            k = d.isoformat()
            x = doris.get(k, 0)
            st.setdefault(k, {})
            m = st[k].get('mongo', 0)
            st[k]['doris'] = x
            if m > 0:
                st[k]['coverage'] = round(x / m, 4)
            st[k]['checked_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
        save_state(st)
        return sum(doris.values())
    except Exception as e:
        log(f"auto-refresh err: {e}")
        return 0


def auto_refresh_thread():
    """每 N 秒自动刷新 Doris；配额耗尽自动停"""
    while True:
        try:
            if _auto_refresh.get('enabled') and _auto_refresh.get('remaining', 0) > 0:
                interval = max(10, int(_auto_refresh.get('interval', 30)))
                sd = _auto_refresh.get('start', '20260401')
                ed = _auto_refresh.get('end', '20260430')
                s = date(int(sd[:4]),int(sd[4:6]),int(sd[6:8]))
                e = date(int(ed[:4]),int(ed[4:6]),int(ed[6:8]))
                total = _auto_refresh_doris(s, e)
                _auto_refresh['last_doris_total_prev'] = _auto_refresh.get('last_doris_total', 0)
                _auto_refresh['last_doris_total'] = total
                _auto_refresh['last_run'] = time.strftime('%Y-%m-%d %H:%M:%S')
                _auto_refresh['remaining'] = max(0, _auto_refresh.get('remaining', 0) - 1)
                if _auto_refresh['remaining'] == 0:
                    log("auto-refresh quota exhausted, pause")
            else:
                interval = 30
        except Exception as e:
            log(f"auto-refresh thread err: {e}")
            interval = 30
        time.sleep(interval)


# ============ Worker ============
def _robust_login(retries=5, sleep_s=10):
    """tok 过期/SP 重启容错: 失败重试，全部失败抛异常由调用方决定"""
    last = None
    for i in range(retries):
        try:
            return sp_login()
        except Exception as e:
            last = e
            log(f"  sp_login retry {i+1}/{retries}: {e}")
            time.sleep(sleep_s)
    raise last or RuntimeError("sp_login failed")


def _run_one_window(app_id, ws, we, parallelism, jm_mem, tm_mem):
    """跑单个 window: 返回 (st_code, dur). st_code: 10=fin, 7=fail, 9=cancel, -1=timeout, -2=exception, -3=stopped"""
    args = f"--start-date {ws.strftime('%Y%m%d')} --end-date {we.strftime('%Y%m%d')}"
    t0 = time.time()
    try:
        tok, _ = _robust_login()
        sp_app_update_args(tok, app_id, args,
                           parallelism=parallelism, jm_mem=jm_mem, tm_mem=tm_mem)
        r = sp_app_start(tok, app_id)
        log(f"  START code={r.get('code')}")
    except Exception as e:
        log(f"  start EXCEPTION: {e}")
        return -2, int(time.time() - t0)

    last_state = None
    consec_fail = 0
    while True:
        if _worker_stop.is_set():
            log("  STOP during window")
            try:
                tok, _ = _robust_login(retries=2, sleep_s=3)
                sp_app_cancel(tok, app_id)
            except Exception: pass
            return -3, int(time.time() - t0)
        time.sleep(15)
        dur = int(time.time() - t0)
        try:
            tok, _ = _robust_login(retries=3, sleep_s=5)
            d_ = sp_app_get(tok, app_id)
            consec_fail = 0
        except Exception as e:
            consec_fail += 1
            if consec_fail >= 8:  # 2 min 都拉不到 -> 视为异常
                log(f"  poll repeated err ({consec_fail}): {e}")
                return -2, dur
            continue
        s = d_.get('state')
        if s != last_state:
            log(f"  dur={dur}s state={s}")
            last_state = s
        if s in (7, 9, 10):
            log(f"  TERMINAL state={s} dur={dur}s")
            return s, dur
        if dur > CFG['window_timeout_seconds']:
            log("  TIMEOUT, cancel")
            try:
                tok, _ = _robust_login(retries=2, sleep_s=3)
                sp_app_cancel(tok, app_id)
            except Exception: pass
            return -1, dur


def worker_loop(app_id, start_d, end_d, step, threshold, rerun_set, rerun_all,
                parallelism=None, jm_mem=None, tm_mem=None):
    global _worker_status
    _worker_status.update(running=True, started_at=time.strftime('%Y-%m-%d %H:%M:%S'),
                          msg="building windows", windows_done=0, app_id=app_id)
    try:
        all_days = []
        cur = start_d
        while cur <= end_d:
            all_days.append(cur)
            cur += timedelta(days=1)
        state = load_state()
        # 清理上次 session 残留的 running (防过夜重启后死锁)
        for k, v in list(state.items()):
            if v.get('status') == 'running':
                v['status'] = 'stale-running'
        save_state(state)
        state = refresh_coverage(state, all_days)
        save_state(state)
        # 最多过 MAX_PASS 轮，每轮把没达标的再跑一遍
        MAX_PASS = 3
        terminal_ok = {'finished', 'all-ok'}
        for pass_idx in range(MAX_PASS):
            if _worker_stop.is_set():
                _worker_status['msg'] = "stopped"
                return
            need_run = []
            for d in all_days:
                k = d.isoformat()
                cov = state.get(k, {}).get('coverage', 0)
                if pass_idx == 0 and (rerun_all or k in rerun_set):
                    need_run.append(d)
                elif cov < threshold:
                    need_run.append(d)
            if not need_run:
                log(f"pass {pass_idx+1}: All days OK, done")
                _worker_status['msg'] = "all OK"
                return
            groups = [[need_run[0]]]
            for d in need_run[1:]:
                if (d - groups[-1][-1]).days == 1:
                    groups[-1].append(d)
                else:
                    groups.append([d])
            windows = []
            for grp in groups:
                for i in range(0, len(grp), step):
                    seg = grp[i:i + step]
                    windows.append((seg[0], seg[-1]))
            _worker_status['windows_total'] = len(windows)
            _worker_status['windows_done'] = 0
            log(f"=== PASS {pass_idx+1}/{MAX_PASS} planned {len(windows)} windows ===")
            for i, (ws, we) in enumerate(windows):
                if _worker_stop.is_set():
                    log(f"STOP before window {i+1}")
                    try:
                        tok, _ = _robust_login(retries=2, sleep_s=3)
                        sp_app_cancel(tok, app_id)
                    except Exception: pass
                    _worker_status['msg'] = "stopped"
                    return
                _worker_status['current_window'] = f"{ws} ~ {we} (pass {pass_idx+1})"
                _worker_status['msg'] = f"P{pass_idx+1} window {i+1}/{len(windows)}"
                log(f"--- P{pass_idx+1} Window {i+1}/{len(windows)} {ws}~{we} ---")
                for d in all_days:
                    if ws <= d <= we:
                        state.setdefault(d.isoformat(), {})['status'] = 'running'
                        state[d.isoformat()]['last_run_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
                save_state(state)
                # 1 个 window 失败本轮跳过下一窗口；外层 pass 会再补
                try:
                    st_code, dur = _run_one_window(app_id, ws, we, parallelism, jm_mem, tm_mem)
                except Exception as e:
                    log(f"  outer EXCEPTION: {e}")
                    st_code, dur = -2, 0
                flag = {10:'finished',7:'failed',9:'canceled',-1:'timeout',-2:'exception',-3:'stopped'}.get(st_code, str(st_code))
                time.sleep(20)  # 等 sink commit + Doris MoW 收尾
                try:
                    state = refresh_coverage(state,
                        [ws + timedelta(days=k) for k in range((we-ws).days+1)])
                    for d in all_days:
                        if ws <= d <= we:
                            state.setdefault(d.isoformat(), {})['status'] = flag
                            state[d.isoformat()]['last_window'] = f"{ws}~{we}"
                    save_state(state)
                except Exception as e:
                    log(f"  refresh err: {e}")
                _worker_status['windows_done'] = i + 1
                if _worker_stop.is_set():
                    _worker_status['msg'] = "stopped"
                    return
                time.sleep(15)
            # 本 pass 跑完，下一 pass 会重检 coverage 决定哪些再跑
            log(f"pass {pass_idx+1} done, recheck coverage")
        _worker_status['msg'] = f"max {MAX_PASS} passes done"
        log(f"all {MAX_PASS} passes done")
    except Exception as e:
        log(f"worker EXCEPTION: {e}")
        _worker_status['msg'] = f"exception: {e}"
    finally:
        _worker_status['running'] = False
        _worker_status['current_window'] = None


# ============ HTML ============
HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>SP Batch Backfill</title>
<style>
body{font-family:'Microsoft YaHei',Arial;font-size:13px;background:#f5f5f5;margin:0;padding:12px}
h2{margin:6px 0}
h3{margin:8px 0 4px}
table{border-collapse:collapse;background:#fff;width:100%}
th,td{border:1px solid #ddd;padding:3px 6px;text-align:right;font-size:12px}
th{background:#e8e8e8;text-align:center}
.left{text-align:left}
.ok{background:#d4f5d4}
.run{background:#ffe9b3}
.fail{background:#fbd4d4}
.box{background:#fff;padding:10px;margin:6px 0;border:1px solid #ddd;border-radius:3px}
.row{display:flex;gap:12px;flex-wrap:wrap}
button{padding:5px 12px;cursor:pointer;font-size:12px}
button:disabled{opacity:0.5}
input[type=text],input[type=number],select{padding:3px 6px;font-size:12px}
input[type=text]{width:140px}
.lab{display:inline-block;width:80px;text-align:right;margin-right:4px}
pre{background:#1e1e1e;color:#e0e0e0;padding:6px;margin:0;height:240px;overflow:auto;font-size:11px;font-family:Consolas,monospace}
.kpi{font-weight:bold;color:#0066cc}
.tab{display:inline-block;padding:4px 12px;background:#e8e8e8;cursor:pointer;margin-right:2px;border-radius:3px 3px 0 0}
.tab.active{background:#fff;border:1px solid #ddd;border-bottom:1px solid #fff;position:relative;top:1px}
.tabp{display:none}.tabp.active{display:block}
.danger{color:#c00}
</style></head>
<body>
<h2>Streampark MongoDB→Doris 批量回灌 <span style="font-size:12px;color:#999">port 18792 · 数据/日志位于 tools/sp_batch_ui/data/</span></h2>

<div class="box">
  <div class="row">
    <div><span class="lab">SP App</span>
      <select id="appsel" onchange="loadAppDetail()" style="width:380px"></select>
      <button onclick="loadApps()">↻</button>
    </div>
    <div><b>state:</b> <span id="appstate" class="kpi">-</span></div>
    <div><a id="splink" href="#" target="_blank" style="font-size:12px">📊 打开 SP UI</a> · <a id="flinklink" href="#" target="_blank" style="font-size:12px">🔗 Flink JM</a></div>
    <div><b>worker:</b> <span id="ws" class="kpi">idle</span> · <span id="wp"></span> · <span id="wc"></span></div>
  </div>
</div>

<div class="row">
  <div class="box" style="flex:1;min-width:600px">
    <div class="row">
      <div><span class="lab">start</span><input id="sd" type="text" value="20260401"></div>
      <div><span class="lab">end</span><input id="ed" type="text" value="20260430"></div>
      <div><span class="lab">step (天)</span><input id="step" type="number" value="3" min="1" max="30" style="width:60px"></div>
      <div><span class="lab">阈值</span><input id="th" type="number" value="0.99" min="0" max="1" step="0.01" style="width:60px"></div>
    </div>
    <div class="row" style="margin-top:6px">
      <div><span class="lab">重跑日期</span><input id="rerun" type="text" placeholder="20260418,20260421" style="width:300px"></div>
      <div><label><input id="rall" type="checkbox"> 全部重跑</label></div>
    </div>
    <div class="row" style="margin-top:6px">
      <div><span class="lab">parallelism</span><input id="par" type="number" value="2" min="1" max="32" style="width:60px" title="写入 SP options.parallelism.default (单源)"></div>
      <div><span class="lab">JM mem</span><input id="jm" type="text" value="4096mb" style="width:90px"></div>
      <div><span class="lab">TM mem</span><input id="tm" type="text" value="8192mb" style="width:90px"></div>
      <div style="font-size:11px;color:#999;align-self:center">并行度走 SP options 单源 (代码不再覆盖)</div>
    </div>
    <div class="row" style="margin-top:8px">
      <button id="bstart">▶ Start Loop</button>
      <button id="bstop">■ Stop</button>
      <button onclick="api('refresh','POST',{start:sd.value,end:ed.value}).then(()=>loadTable())">↻ 全量刷新 (Mongo+Doris,慢)</button>
      <button onclick="api('refresh_doris','POST',{start:sd.value,end:ed.value}).then(()=>loadTable())">⚡ 立刻刷一次 Doris (快)</button>
      <label style="margin-left:12px"><input id="autoref" type="checkbox" checked> 后台自动刷新 间隔</label>
      <input id="autoint" type="number" value="30" min="10" max="600" style="width:60px"> 秒
      <button onclick="saveAuto()">保存</button>
      <span id="autostat" style="font-size:11px;color:#666"></span>
    </div>
    <h3>每日覆盖率</h3>
    <table id="t"><thead><tr><th class="left">date</th><th>mongo</th><th>doris</th><th>cov%</th><th>status</th><th>last</th></tr></thead><tbody></tbody></table>
    <div style="margin-top:6px"><b>总计:</b> <span id="tot" class="kpi">-</span></div>
  </div>

  <div class="box" style="flex:1;min-width:600px">
    <div>
      <span class="tab active" onclick="setTab('log')">实时日志</span>
      <span class="tab" onclick="setTab('jm')">JM 启动日志</span>
      <span class="tab" onclick="setTab('app')">启动时实际参数</span>
      <span class="tab" onclick="setTab('clear')">数据清空</span>
    </div>
    <div id="tab-log" class="tabp active"><pre id="log"></pre></div>
    <div id="tab-jm" class="tabp"><button onclick="loadJM()">拉 JM 启动日志</button>
      <div style="font-size:11px;color:#c00">⚠ SP API <code>/k8sStartLog</code> 仅返回 pod 启动头几百行，运行中/失败后的真实日志在 Flink Web UI:
        <a id="jmwebui" target="_blank" style="font-weight:bold">→ 打开 Flink Web UI (JobManager → Log)</a></div>
      <pre id="jm"></pre></div>
    <div id="tab-app" class="tabp"><button onclick="loadAppDetail()">刷新参数</button>
      <pre id="appdet"></pre></div>
    <div id="tab-clear" class="tabp">
      <h3 class="danger">⚠ 数据清空 (危险操作, 仅测试用)</h3>
      <div>清空指定日期 Doris 分区 (drop + add 重建): </div>
      <div style="margin-top:6px">
        <span class="lab">日期</span><input id="cleardays" type="text" placeholder="20260418,20260421 逗号分隔" style="width:300px">
        <button onclick="doClear()" class="danger">DROP & RECREATE</button>
      </div>
      <div style="margin-top:6px;font-size:11px;color:#666">
        等效 SQL:<br>
        <code>ALTER TABLE uat_prod_ods_mongo_magnolia.event_data DROP PARTITION p20260418;</code><br>
        <code>ALTER TABLE uat_prod_ods_mongo_magnolia.event_data ADD PARTITION p20260418 VALUES [('2026-04-18'), ('2026-04-19'));</code>
      </div>
      <pre id="clearout" style="height:120px"></pre>
    </div>
  </div>
</div>

<script>
async function api(path, method='GET', body=null){
  const opts = {method, headers:{'Content-Type':'application/json'}}
  if(body) opts.body = JSON.stringify(body)
  const r = await fetch('/api/'+path, opts)
  return r.json()
}
const fmt = n => (n||0).toLocaleString()
function setTab(t){
  for(const el of document.querySelectorAll('.tab')) el.classList.remove('active')
  for(const el of document.querySelectorAll('.tabp')) el.classList.remove('active')
  event.target.classList.add('active')
  document.getElementById('tab-'+t).classList.add('active')
}
async function loadApps(){
  const d = await api('apps')
  const sel = document.getElementById('appsel')
  const prev = sel.value
  sel.innerHTML = ''
  for(const a of d.apps){
    const o = document.createElement('option')
    o.value = a.id
    o.textContent = `${a.id} - ${a.jobName} ${a.is_batch?'★batch':''} [state=${a.state}]`
    sel.appendChild(o)
  }
  if(prev) sel.value = prev
  else if(d.default_id) sel.value = d.default_id
  loadAppDetail()
}
async function loadAppDetail(){
  const id = document.getElementById('appsel').value
  if(!id) return
  const d = await api('app_detail?id='+id)
  document.getElementById('appstate').textContent = d.state || '-'
  document.getElementById('appdet').textContent =
    `id:        ${d.id}\njobName:   ${d.jobName}\nstate:     ${d.state}\nrelease:   ${d.release}\nstartTime: ${d.startTime}\nendTime:   ${d.endTime}\nduration:  ${d.duration} ms\nflinkRestUrl: ${d.flinkRestUrl||'-'}\nk8sRestExposedType: ${d.k8sRestExposedType||'-'} (1=ClusterIP内部,2=NodePort外部)\n\nargs:\n${d.args}\n\noptions:\n${d.options}\n\ndynamicProperties:\n${d.dynamicProperties}`
  document.getElementById('splink').href = `http://10.0.0.121:31000/#/flink/app/detail?appId=${id}`
  // SP 自带 /proxy/flink/{id}/ 代理 Flink Web UI, 浏览器登录态下可访问
  const proxyUrl = `http://10.0.0.121:31000/proxy/flink/${id}/#/overview`
  document.getElementById('flinklink').href = proxyUrl
  document.getElementById('flinklink').textContent = '🔗 Flink Web UI (SP代理)'
  document.getElementById('jmwebui').href = `http://10.0.0.121:31000/proxy/flink/${id}/#/job-manager/log`
}
async function loadTable(){
  const sd = document.getElementById('sd').value
  const ed = document.getElementById('ed').value
  const d = await api(`state?start=${sd}&end=${ed}`)
  const tb = document.querySelector('#t tbody')
  tb.innerHTML = ''
  let tm=0, td=0
  for(const r of d.days){
    let cls = ''
    if(r.status === 'running') cls='run'
    else if(r.status === 'finished' || r.coverage >= 0.99) cls='ok'
    else if(['failed','timeout','exception','canceled'].includes(r.status)) cls='fail'
    const tr = document.createElement('tr'); tr.className = cls
    tr.innerHTML = `<td class="left">${r.date}</td><td>${fmt(r.mongo)}</td><td>${fmt(r.doris)}</td><td>${(r.coverage*100).toFixed(2)}%</td><td>${r.status||'-'}</td><td>${(r.last_run_at||'-').slice(11,19)}</td>`
    tb.appendChild(tr)
    tm += r.mongo; td += r.doris
  }
  document.getElementById('tot').textContent = `mongo=${fmt(tm)} doris=${fmt(td)} = ${tm>0?(td/tm*100).toFixed(2):0}%`
}
async function pollStatus(){
  const s = await api('status')
  document.getElementById('ws').textContent = s.msg + (s.running?' (RUN)':'')
  document.getElementById('wc').textContent = s.current_window || ''
  document.getElementById('wp').textContent = s.windows_total>0 ? `${s.windows_done}/${s.windows_total}` : ''
  document.getElementById('bstart').disabled = s.running
  document.getElementById('bstop').disabled = !s.running
  const log = await api('log')
  const pre = document.getElementById('log')
  pre.textContent = log.text
  pre.scrollTop = pre.scrollHeight
}
async function loadJM(){
  const id = document.getElementById('appsel').value
  const d = await api('jmlog?id='+id)
  document.getElementById('jm').textContent = d.log || '(empty / pod 未启动或已 gone)'
}
async function doClear(){
  const days = document.getElementById('cleardays').value
  if(!days){alert('请填日期'); return}
  if(!confirm(`确认清空 Doris 分区: ${days}?`)) return
  const d = await api('clear_partitions','POST',{days})
  document.getElementById('clearout').textContent = JSON.stringify(d, null, 2)
  loadTable()
}
document.getElementById('bstart').onclick = async () => {
  const body = {
    app_id: document.getElementById('appsel').value,
    start: document.getElementById('sd').value,
    end: document.getElementById('ed').value,
    step: +document.getElementById('step').value,
    threshold: +document.getElementById('th').value,
    rerun: document.getElementById('rerun').value,
    rerun_all: document.getElementById('rall').checked,
    parallelism: +document.getElementById('par').value,
    jm_mem: document.getElementById('jm').value,
    tm_mem: document.getElementById('tm').value,
  }
  const r = await api('start','POST',body)
  alert(r.msg)
  pollStatus()
}
document.getElementById('bstop').onclick = async () => {
  const r = await api('stop','POST')
  alert(r.msg)
}
async function saveAuto(){
  const body = {
    enabled: document.getElementById('autoref').checked,
    interval: +document.getElementById('autoint').value,
    start: document.getElementById('sd').value,
    end: document.getElementById('ed').value,
  }
  const r = await api('auto','POST',body)
  alert(r.msg)
  loadAutoStatus()
}
async function loadAutoStatus(){
  const a = await api('auto')
  document.getElementById('autoref').checked = a.enabled
  document.getElementById('autoint').value = a.interval
  let delta = ''
  if(a.last_doris_total_prev>0){
    const d = a.last_doris_total - a.last_doris_total_prev
    delta = ` (Δ${d>=0?'+':''}${d.toLocaleString()})`
  }
  const rem = (a.remaining||0)
  const quotaTxt = rem>0 ? `<span style="color:#0a0">剩余配额 ${rem}</span>` : `<span style="color:#c00">⚠ 配额耗尽 已暂停</span>`
  document.getElementById('autostat').innerHTML = (a.last_run ? `last=${a.last_run} doris总=${(a.last_doris_total||0).toLocaleString()}${delta} · ` : '(等首轮) · ') + quotaTxt
}
loadApps(); loadTable(); pollStatus(); loadAutoStatus()
setInterval(loadTable, 8000)        // 表格 8s 刷新（依赖 state 文件被后台线程实时更新）
setInterval(pollStatus, 3000)
setInterval(loadAutoStatus, 6000)
</script>
</body></html>
"""


# ============ HTTP Handler ============
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a, **kw): pass

    def _send(self, code, body, ctype='application/json'):
        b = body.encode('utf-8') if isinstance(body, str) else body
        self.send_response(code)
        self.send_header('Content-Type', ctype + '; charset=utf-8')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _parse_body(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length).decode('utf-8') if length else '{}'
        try: return json.loads(body or '{}')
        except Exception: return {}

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path in ('/', '/index.html'):
            return self._send(200, HTML, 'text/html')
        if u.path == '/api/status':
            return self._send(200, json.dumps(_worker_status))
        if u.path == '/api/log':
            return self._send(200, json.dumps({'text': read_log_tail()}))
        if u.path == '/api/auto':
            return self._send(200, json.dumps(_auto_refresh))
        if u.path == '/api/apps':
            try:
                tok, team = sp_login()
                apps = sp_list_batch_apps(tok, team)
                return self._send(200, json.dumps({'apps': apps, 'default_id': CFG['streampark']['default_app_id']}))
            except Exception as e:
                return self._send(500, json.dumps({'err': str(e)}))
        if u.path == '/api/app_detail':
            qs = urllib.parse.parse_qs(u.query)
            app_id = qs.get('id', [''])[0]
            try:
                tok, _ = sp_login()
                d = sp_app_get(tok, app_id)
                return self._send(200, json.dumps({
                    'id': d.get('id'), 'jobName': d.get('jobName'),
                    'state': d.get('state'), 'release': d.get('release'),
                    'startTime': d.get('startTime'), 'endTime': d.get('endTime'),
                    'duration': d.get('duration'),
                    'args': d.get('args',''), 'options': d.get('options',''),
                    'dynamicProperties': d.get('dynamicProperties',''),
                    'flinkRestUrl': d.get('flinkRestUrl',''),
                    'k8sRestExposedType': d.get('k8sRestExposedType'),
                }))
            except Exception as e:
                return self._send(500, json.dumps({'err': str(e)}))
        if u.path == '/api/jmlog':
            qs = urllib.parse.parse_qs(u.query)
            app_id = qs.get('id', [''])[0]
            try:
                tok, _ = sp_login()
                logtxt = sp_app_jmlog(tok, app_id)
                return self._send(200, json.dumps({'log': logtxt[-50000:]}))
            except Exception as e:
                return self._send(500, json.dumps({'err': str(e), 'log': ''}))
        if u.path == '/api/state':
            qs = urllib.parse.parse_qs(u.query)
            sd = qs.get('start', ['20260401'])[0]
            ed = qs.get('end', ['20260430'])[0]
            try:
                s = date(int(sd[:4]), int(sd[4:6]), int(sd[6:8]))
                e = date(int(ed[:4]), int(ed[4:6]), int(ed[6:8]))
            except Exception:
                return self._send(400, json.dumps({'err':'bad date'}))
            days = []; cur = s
            while cur <= e: days.append(cur); cur += timedelta(days=1)
            st = load_state()
            out = [{'date': d.isoformat(),
                    'mongo': st.get(d.isoformat(),{}).get('mongo',0),
                    'doris': st.get(d.isoformat(),{}).get('doris',0),
                    'coverage': st.get(d.isoformat(),{}).get('coverage',0),
                    'status': st.get(d.isoformat(),{}).get('status',''),
                    'last_run_at': st.get(d.isoformat(),{}).get('last_run_at','')} for d in days]
            return self._send(200, json.dumps({'days': out}))
        return self._send(404, json.dumps({'err':'not found'}))

    def do_POST(self):
        global _worker_thread, _worker_stop
        u = urllib.parse.urlparse(self.path)
        j = self._parse_body()
        if u.path == '/api/start':
            if _worker_status.get('running'):
                return self._send(200, json.dumps({'ok':False,'msg':'already running'}))
            try:
                sd = j.get('start','20260401'); ed = j.get('end','20260430')
                step = int(j.get('step', 3)); th = float(j.get('threshold', 0.99))
                rerun_str = j.get('rerun','') or ''
                rerun_all = bool(j.get('rerun_all', False))
                app_id = str(j.get('app_id') or CFG['streampark']['default_app_id'])
                s = date(int(sd[:4]),int(sd[4:6]),int(sd[6:8]))
                e = date(int(ed[:4]),int(ed[4:6]),int(ed[6:8]))
                rerun_set = set()
                for x in rerun_str.split(','):
                    x = x.strip()
                    if x: rerun_set.add(date(int(x[:4]),int(x[4:6]),int(x[6:8])).isoformat())
            except Exception as ex:
                return self._send(400, json.dumps({'ok':False,'msg':f'bad input: {ex}'}))
            par = j.get('parallelism')
            jm_mem = (j.get('jm_mem') or '').strip() or None
            tm_mem = (j.get('tm_mem') or '').strip() or None
            _worker_stop.clear()
            _worker_thread = threading.Thread(target=worker_loop,
                args=(app_id,s,e,step,th,rerun_set,rerun_all,par,jm_mem,tm_mem), daemon=True)
            _worker_thread.start()
            log(f"START app={app_id} {sd}~{ed} step={step} th={th} par={par} jm={jm_mem} tm={tm_mem} rerun={rerun_str} all={rerun_all}")
            return self._send(200, json.dumps({'ok':True,'msg':'started'}))
        if u.path == '/api/stop':
            _worker_stop.set()
            log("STOP requested by user")
            return self._send(200, json.dumps({'ok':True,'msg':'stop signal sent'}))
        if u.path == '/api/auto':
            _auto_refresh['enabled'] = bool(j.get('enabled', True))
            _auto_refresh['interval'] = max(10, int(j.get('interval', 30)))
            _auto_refresh['start'] = j.get('start','20260401')
            _auto_refresh['end'] = j.get('end','20260430')
            log(f"auto-refresh updated: enabled={_auto_refresh['enabled']} interval={_auto_refresh['interval']}s range={_auto_refresh['start']}~{_auto_refresh['end']}")
            return self._send(200, json.dumps({'ok':True, 'msg':'auto refresh updated'}))
        if u.path == '/api/refresh':
            sd = j.get('start','20260401'); ed = j.get('end','20260430')
            s = date(int(sd[:4]),int(sd[4:6]),int(sd[6:8]))
            e = date(int(ed[:4]),int(ed[4:6]),int(ed[6:8]))
            days = []; cur = s
            while cur <= e: days.append(cur); cur += timedelta(days=1)
            try:
                st = refresh_coverage(load_state(), days)
                save_state(st)
                return self._send(200, json.dumps({'ok':True,'msg':'refreshed'}))
            except Exception as ex:
                return self._send(500, json.dumps({'ok':False,'msg':str(ex)}))
        if u.path == '/api/refresh_doris':
            sd = j.get('start','20260401'); ed = j.get('end','20260430')
            try:
                s = date(int(sd[:4]),int(sd[4:6]),int(sd[6:8]))
                e = date(int(ed[:4]),int(ed[4:6]),int(ed[6:8]))
                total = _auto_refresh_doris(s, e)
                _auto_refresh['last_run'] = time.strftime('%Y-%m-%d %H:%M:%S')
                _auto_refresh['last_doris_total_prev'] = _auto_refresh.get('last_doris_total',0)
                _auto_refresh['last_doris_total'] = total
                # 手动刷新 -> 配额重置为 reload_on_click
                _auto_refresh['remaining'] = int(_auto_refresh.get('reload_on_click', 1000))
                log(f"manual doris refresh -> remaining reset to {_auto_refresh['remaining']}")
                return self._send(200, json.dumps({'ok':True, 'msg': f'doris total={total}, remaining={_auto_refresh["remaining"]}'}))
            except Exception as ex:
                return self._send(500, json.dumps({'ok':False, 'msg':str(ex)}))
        if u.path == '/api/clear_partitions':
            days_str = j.get('days','') or ''
            results = []
            for x in days_str.split(','):
                x = x.strip()
                if not x: continue
                try:
                    d = date(int(x[:4]),int(x[4:6]),int(x[6:8]))
                    ok, msg = doris_drop_partition(d)
                    log(f"clear partition {d}: ok={ok} {msg[:120]}")
                    results.append({'date': d.isoformat(), 'ok': ok, 'sql': msg})
                except Exception as e:
                    results.append({'date': x, 'ok': False, 'sql': str(e)})
            return self._send(200, json.dumps({'results': results}))
        return self._send(404, json.dumps({'err':'not found'}))


class ThreadingTCP(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True


def ensure_single_instance():
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE) as f:
                pid = int((f.read() or '0').strip())
            if pid > 0 and sys.platform == 'win32':
                import ctypes
                h = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
                if h:
                    ctypes.windll.kernel32.CloseHandle(h)
                    print(f"ERROR: app already running (pid={pid}). Exit.")
                    sys.exit(1)
        except Exception:
            pass
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))


def main():
    ensure_single_instance()
    log(f"=== sp_batch_ui app starting on http://localhost:{CFG['port']} (pid={os.getpid()}) ===")
    log(f"App dir: {APP_DIR}")
    log(f"Data dir: {DATA_DIR}")
    # 启动后台 Doris 自动刷新线程
    threading.Thread(target=auto_refresh_thread, daemon=True).start()
    log(f"auto-refresh thread started (interval={_auto_refresh['interval']}s, enabled={_auto_refresh['enabled']})")
    try:
        srv = ThreadingTCP((CFG['host'], CFG['port']), Handler)
        log(f"open http://localhost:{CFG['port']} in browser")
        srv.serve_forever()
    finally:
        try: os.remove(PID_FILE)
        except Exception: pass


if __name__ == '__main__':
    main()
