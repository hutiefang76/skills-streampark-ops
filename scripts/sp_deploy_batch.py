"""创建 / 复用 StreamPark MongoDB→Doris 批量回灌应用。

用法:
    python scripts/sp_deploy_batch.py --env uat --start 20260501 --end 20260507
    python scripts/sp_deploy_batch.py --env uat --start 20260501 --end 20260507 --dry-run

参数:
    --jar-module / --main-class / --flink-image 可覆盖默认值（按业务调整）
    --nacos-addr / --nacos-data-id 注入 dynamicProperties

默认值参考 realtime-job 项目，适用于 KDWL Mongo→Doris 批任务。其他业务可改默认参数。
"""
import os
import sys
import json
import argparse

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from lib.sp_client import SPClient, SPError


DEFAULT_JOB_NAME = "mongo-batch-{start}-{end}"
DEFAULT_MAIN_CLASS = "com.liaoliao.car.rose.MongoDbToDorisBatchJob"
DEFAULT_FLINK_IMAGE = "10.0.0.15:58070/public/apache/flink:1.20.3-scala_2.12-java11-shanghai-s3"
DEFAULT_MODULE = "rose-realtime-flink-1.0-SNAPSHOT"
DEFAULT_JAR = "rose-realtime-flink-1.0-SNAPSHOT.jar"
DEFAULT_NACOS_ADDR = "10.0.0.121:31531"
DEFAULT_NACOS_DATA_ID = "realtime-job-data-prod.yaml"
DEFAULT_NS = "streampark-job"
DEFAULT_PROJECT_ID = "100001"
DEFAULT_VERSION_ID = "100001"


def build_payload(start: str, end: str, args_ns) -> dict:
    job_name = args_ns.job_name or DEFAULT_JOB_NAME.format(start=start, end=end)
    arg_str = f"--start-date {start} --end-date {end}"
    if args_ns.rules:
        arg_str += f" --rules {args_ns.rules}"
    if args_ns.parallelism:
        arg_str += f" --parallelism {args_ns.parallelism}"

    dyn_props = "\n".join([
        f"-DFLINK_ENV={args_ns.flink_env}",
        f"-DNACOS_ADDR={args_ns.nacos_addr}",
        "-DNACOS_USERNAME=nacos",
        f"-DNACOS_PASSWORD={args_ns.nacos_password}",
        f"-DNACOS_DATA_ID={args_ns.nacos_data_id}",
        "-Dkubernetes.service-account=streampark",
        "-Dpipeline.object-reuse=true",
        "-Drestart-strategy.type=none",
        "-Dexecution.runtime-mode=BATCH",
    ])

    options = json.dumps({
        "parallelism.default": int(args_ns.parallelism or 2),
        "jobmanager.memory.process.size": args_ns.jm_mem,
        "taskmanager.memory.process.size": args_ns.tm_mem,
    })

    return {
        "jobName": job_name,
        "jobType": 1,
        "executionMode": 6,
        "projectId": args_ns.project_id,
        "versionId": args_ns.version_id,
        "module": args_ns.module,
        "jar": args_ns.jar,
        "mainClass": args_ns.main_class,
        "args": arg_str,
        "options": options,
        "dynamicProperties": dyn_props,
        "flinkImage": args_ns.flink_image,
        "k8sNamespace": args_ns.namespace,
        "resolveOrder": 1,
        "k8sRestExposedType": 1,
        "hotParams": json.dumps({"kubernetes.service-account": "streampark"}),
        "appType": 2,
        "resourceFrom": 1,
        "description": f"MongoDB batch backfill {start}~{end} (auto-created by sp_deploy_batch.py)",
        "tags": "batch,mongo,backfill",
        "restoreOrTriggerSavepoint": "false",
        "allowNonRestored": "false",
        "drain": "false",
        "k8sHadoopIntegration": "false",
        "savepointTimeout": "60",
    }


def main():
    p = argparse.ArgumentParser(description='Deploy MongoDB→Doris batch app to StreamPark')
    p.add_argument('--env', required=True)
    p.add_argument('--start', required=True, help='YYYYMMDD start date')
    p.add_argument('--end', required=True, help='YYYYMMDD end date')
    p.add_argument('--dry-run', action='store_true')

    # Customization
    p.add_argument('--job-name', default='', help='override job name (default: mongo-batch-<start>-<end>)')
    p.add_argument('--main-class', default=DEFAULT_MAIN_CLASS)
    p.add_argument('--flink-image', default=DEFAULT_FLINK_IMAGE)
    p.add_argument('--module', default=DEFAULT_MODULE)
    p.add_argument('--jar', default=DEFAULT_JAR)
    p.add_argument('--namespace', default=DEFAULT_NS)
    p.add_argument('--project-id', default=DEFAULT_PROJECT_ID)
    p.add_argument('--version-id', default=DEFAULT_VERSION_ID)

    # Runtime
    p.add_argument('--parallelism', default='2')
    p.add_argument('--rules', default='', help='comma-separated rules')
    p.add_argument('--jm-mem', default='2048mb')
    p.add_argument('--tm-mem', default='4096mb')
    p.add_argument('--flink-env', default='uat')
    p.add_argument('--nacos-addr', default=DEFAULT_NACOS_ADDR)
    p.add_argument('--nacos-data-id', default=DEFAULT_NACOS_DATA_ID)
    p.add_argument('--nacos-password', default='5KI0WkQGkTi9m62vHYNI',
                   help='inject into dynamicProperties (UAT default)')

    args = p.parse_args()

    try:
        sp = SPClient.from_config(args.env)
        sp.login()
    except SPError as e:
        print(f'[FAIL] {e}', file=sys.stderr)
        sys.exit(2)

    print(f'[login] OK user_id={sp.user_id} team_id={sp.team_id}')

    job_name = args.job_name or DEFAULT_JOB_NAME.format(start=args.start, end=args.end)
    existing = sp.find_app(job_name)
    if existing:
        app_id = existing['id']
        print(f'[exists] app id={app_id} state={existing.get("state")} '
              f'release={existing.get("release")} — reuse')
    elif args.dry_run:
        payload = build_payload(args.start, args.end, args)
        print(f'[dry-run] would create app: {job_name}')
        print(json.dumps({k: v for k, v in payload.items() if k in
                          ('jobName', 'mainClass', 'args', 'flinkImage')},
                         indent=2, ensure_ascii=False))
        return
    else:
        payload = build_payload(args.start, args.end, args)
        r = sp.create_app(payload)
        print(f'[create] resp: {r}')
        if r.get('status') == 'error':
            sys.exit(1)
        ex = sp.find_app(job_name)
        if not ex:
            print('[FAIL] app created but not found in list')
            sys.exit(1)
        app_id = ex['id']
        print(f'[create] OK id={app_id} name={job_name}')

    print(f'\nNext: release (build) -> start. App id = {app_id}')
    print(f'UI: {sp.base}/#/flink/app')


if __name__ == '__main__':
    main()
