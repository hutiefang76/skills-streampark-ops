"""显示单个 StreamPark 应用详情 — 只读。

用法:
    python scripts/sp_app_show.py --env uat --name mongo-batch-20260501-20260507
    python scripts/sp_app_show.py --env uat --id 100021
    python scripts/sp_app_show.py --env uat --name realtime-mongodb-doris-prod --json
"""
import os
import sys
import json
import argparse

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from lib.sp_client import SPClient, SPError


def main():
    p = argparse.ArgumentParser(description='Show single StreamPark app details')
    p.add_argument('--env', required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--name', help='exact jobName')
    g.add_argument('--id', type=int, help='app id')
    p.add_argument('--json', action='store_true')
    args = p.parse_args()

    try:
        sp = SPClient.from_config(args.env)
        sp.login()
        if args.name:
            app = sp.find_app(args.name)
        else:
            apps = sp.list_apps()
            app = next((a for a in apps if a.get('id') == args.id), None)
    except SPError as e:
        print(f'[FAIL] {e}', file=sys.stderr)
        sys.exit(2)

    if not app:
        target = args.name or f'id={args.id}'
        print(f'(no app matches {target} in env={args.env})')
        sys.exit(1)

    if args.json:
        print(json.dumps(app, ensure_ascii=False, indent=2))
        return

    print(f"id           : {app.get('id')}")
    print(f"jobName      : {app.get('jobName')}")
    print(f"state        : {app.get('state')}")
    print(f"release      : {app.get('release')}")
    print(f"executionMode: {app.get('executionMode')}  (6=k8s-application)")
    print(f"jobType      : {app.get('jobType')}  (1=jar 2=sql)")
    print(f"mainClass    : {app.get('mainClass')}")
    print(f"jar          : {app.get('jar')}")
    print(f"args         : {app.get('args')}")
    print(f"k8sNamespace : {app.get('k8sNamespace')}")
    print(f"flinkImage   : {app.get('flinkImage')}")
    print(f"description  : {app.get('description')}")
    print(f"tags         : {app.get('tags')}")
    options = app.get('options')
    if options:
        try:
            parsed = json.loads(options) if isinstance(options, str) else options
            print(f"options      : {json.dumps(parsed, indent=2, ensure_ascii=False)}")
        except Exception:
            print(f"options      : {options}")
    dyn = app.get('dynamicProperties')
    if dyn:
        print(f"dynamicProperties:")
        for line in dyn.split('\n'):
            if line.strip():
                print(f"    {line}")


if __name__ == '__main__':
    main()
