"""列出 StreamPark 应用 — 只读，连通性自检命令。

用法:
    python scripts/sp_apps_list.py --env uat
    python scripts/sp_apps_list.py --env uat --filter mongo
    python scripts/sp_apps_list.py --env uat --json
"""
import os
import sys
import argparse
import json

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from lib.sp_client import SPClient, SPError


STATE_NAMES = {
    -1: 'ADDED', 0: 'CREATED', 1: 'FAILED', 2: 'CANCELED', 3: 'RUNNING',
    4: 'FINISHED', 5: 'SUSPENDED', 6: 'RESTARTING', 7: 'STOPPED',
    8: 'FAILING', 9: 'CANCELLING', 10: 'INITIALIZING', 11: 'RECONCILING',
    12: 'LOST', 13: 'MAPPING', 14: 'OTHER', 15: 'REVOKED', 16: 'TERMINATED',
}


def main():
    p = argparse.ArgumentParser(description='List StreamPark Flink apps')
    p.add_argument('--env', required=True, help='environment name (uat / prod / ...)')
    p.add_argument('--filter', default='', help='substring filter on jobName')
    p.add_argument('--json', action='store_true', help='dump raw JSON')
    args = p.parse_args()

    try:
        sp = SPClient.from_config(args.env)
        sp.login()
    except SPError as e:
        print(f'[FAIL] {e}', file=sys.stderr)
        sys.exit(2)

    apps = sp.list_apps(job_name=args.filter)
    if args.json:
        print(json.dumps(apps, ensure_ascii=False, indent=2))
        return

    if not apps:
        print(f'(no apps in env={args.env}{" filter="+args.filter if args.filter else ""})')
        return

    print(f'env={args.env}  team_id={sp.team_id}  count={len(apps)}')
    print(f"{'ID':>8}  {'STATE':>12}  {'RELEASE':>4}  JOB_NAME")
    print('-' * 80)
    for a in apps:
        st = a.get('state')
        st_label = STATE_NAMES.get(st, str(st))
        print(f"{a.get('id', '?'):>8}  {st_label:>12}  {a.get('release', '?'):>4}  {a.get('jobName', '')}")


if __name__ == '__main__':
    main()
