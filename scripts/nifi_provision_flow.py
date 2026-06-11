"""
Собирает в NiFi демо-flow «Kafka → MinIO» через REST API (NiFi 2.x).

    ConsumeKafka (топик sales_events) → MergeContent (пачки по 100)
        → PutS3Object (s3://warehouse/nifi-landing/)

Запуск с хоста (нужен только requests):
    python3 scripts/nifi_provision_flow.py

Идемпотентен: если process group `kafka_to_minio` уже существует — выходит.
Адрес/креды берёт из env (NIFI_URL, NIFI_USER, NIFI_PASSWORD) или дефолты .env.
Тот же flow лежит экспортом в flows/nifi/kafka_to_minio.json — его можно
импортировать в UI руками, без скрипта.
"""
import os
import sys

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_URL = os.environ.get("NIFI_URL", "https://localhost:8443")
NIFI_USER = os.environ.get("NIFI_USER", "nifi")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD", "nifinifinifi")
API = f"{NIFI_URL}/nifi-api"
FLOW_NAME = "kafka_to_minio"

# адреса изнутри docker-сети: NiFi ходит в kafka и minio по именам сервисов
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "sales_events"
S3_ENDPOINT = "http://minio:9000"
BUCKET = "warehouse"
S3_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
S3_SECRET = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

session = requests.Session()
session.verify = False


def login() -> None:
    resp = session.post(f"{API}/access/token",
                        data={"username": NIFI_USER, "password": NIFI_PASSWORD},
                        timeout=30)
    resp.raise_for_status()
    session.headers["Authorization"] = f"Bearer {resp.text}"


def post(path: str, payload: dict) -> dict:
    resp = session.post(f"{API}{path}", json=payload, timeout=30)
    if resp.status_code >= 400:
        sys.exit(f"POST {path} → {resp.status_code}: {resp.text}")
    return resp.json()


def put(path: str, payload: dict) -> dict:
    resp = session.put(f"{API}{path}", json=payload, timeout=30)
    if resp.status_code >= 400:
        sys.exit(f"PUT {path} → {resp.status_code}: {resp.text}")
    return resp.json()


def new_processor(pg: str, ptype: str, name: str, x: int, y: int,
                  properties: dict, auto_terminate: list[str] | None = None) -> dict:
    proc = post(f"/process-groups/{pg}/processors", {
        "revision": {"version": 0},
        "component": {"type": ptype, "name": name, "position": {"x": x, "y": y}},
    })
    config = {"properties": properties}
    if auto_terminate:
        config["autoTerminatedRelationships"] = auto_terminate
    return put(f"/processors/{proc['id']}", {
        "revision": proc["revision"],
        "component": {"id": proc["id"], "config": config},
    })


def connect(pg: str, src: dict, dst: dict, relationship: str) -> None:
    post(f"/process-groups/{pg}/connections", {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src["id"], "groupId": pg, "type": "PROCESSOR"},
            "destination": {"id": dst["id"], "groupId": pg, "type": "PROCESSOR"},
            "selectedRelationships": [relationship],
        },
    })


def main() -> None:
    login()
    root = session.get(f"{API}/flow/process-groups/root", timeout=30).json()["processGroupFlow"]["id"]

    existing = session.get(f"{API}/process-groups/{root}/process-groups", timeout=30).json()["processGroups"]
    for p in existing:
        if p["component"]["name"] == FLOW_NAME:
            print(f"flow «{FLOW_NAME}» уже существует ({p['id']}) — выходим")
            return

    pg = post(f"/process-groups/{root}/process-groups", {
        "revision": {"version": 0},
        "component": {"name": FLOW_NAME, "position": {"x": 100, "y": 100}},
    })["id"]
    print(f"process group: {pg}")

    # --- controller services -------------------------------------------------
    kafka_cs = post(f"/process-groups/{pg}/controller-services", {
        "revision": {"version": 0},
        "component": {"type": "org.apache.nifi.kafka.service.Kafka3ConnectionService",
                      "name": "kafka-datastack"},
    })
    put(f"/controller-services/{kafka_cs['id']}", {
        "revision": kafka_cs["revision"],
        "component": {"id": kafka_cs["id"],
                      "properties": {"bootstrap.servers": KAFKA_BOOTSTRAP}},
    })

    aws_cs = post(f"/process-groups/{pg}/controller-services", {
        "revision": {"version": 0},
        "component": {"type": "org.apache.nifi.processors.aws.credentials.provider.service."
                              "AWSCredentialsProviderControllerService",
                      "name": "minio-creds"},
    })
    put(f"/controller-services/{aws_cs['id']}", {
        "revision": aws_cs["revision"],
        "component": {"id": aws_cs["id"],
                      "properties": {"Access Key ID": S3_KEY, "Secret Access Key": S3_SECRET}},
    })

    for cs_id in (kafka_cs["id"], aws_cs["id"]):
        cs = session.get(f"{API}/controller-services/{cs_id}", timeout=30).json()
        put(f"/controller-services/{cs_id}/run-status",
            {"revision": cs["revision"], "state": "ENABLED"})
    print("controller services включены")

    # --- processors ----------------------------------------------------------
    consume = new_processor(
        pg, "org.apache.nifi.kafka.processors.ConsumeKafka", "ConsumeKafka sales_events",
        0, 0,
        {
            "Kafka Connection Service": kafka_cs["id"],
            "Group ID": "nifi",
            "Topics": TOPIC,
            "auto.offset.reset": "earliest",
        },
    )

    merge = new_processor(
        pg, "org.apache.nifi.processors.standard.MergeContent", "MergeContent 100",
        0, 250,
        {
            "Minimum Number of Entries": "100",
            "Maximum Number of Entries": "1000",
            "Max Bin Age": "30 sec",
            "Delimiter Strategy": "Text",
            "Demarcator": "\n",
        },
        auto_terminate=["original", "failure"],
    )

    puts3 = new_processor(
        pg, "org.apache.nifi.processors.aws.s3.PutS3Object", "PutS3Object nifi-landing",
        0, 500,
        {
            "Bucket": BUCKET,
            "Object Key": "nifi-landing/${filename}",
            "Region": "us-east-1",
            "AWS Credentials Provider Service": aws_cs["id"],
            "Endpoint Override URL": S3_ENDPOINT,
            "Use Path Style Access": "true",
        },
        auto_terminate=["success", "failure"],
    )

    connect(pg, consume, merge, "success")
    connect(pg, merge, puts3, "merged")
    print("процессоры и связи созданы")

    put(f"/flow/process-groups/{pg}", {"id": pg, "state": "RUNNING"})
    print(f"flow «{FLOW_NAME}» запущен. UI: {NIFI_URL}/nifi, файлы: s3://{BUCKET}/nifi-landing/")


if __name__ == "__main__":
    main()
