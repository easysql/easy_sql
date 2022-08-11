import json
from datetime import datetime
from typing import Any


class EsService:
    def __init__(self, base_url: str, should_send: bool = True):
        self.base_url = base_url.strip("/")
        self.should_send = should_send
        self.data = None

    def post(self, url_path: str, data: str):
        import requests

        if self.should_send:
            resp = requests.post(
                self.base_url + url_path, headers={"Content-Type": "Application/json"}, data=data.encode("utf8")
            )
            if not resp.ok:
                raise Exception(f"send data quality report failed(status={resp.status_code}): {resp.text}")
            print("data post to es done")
        else:
            self.data = {"method": "post", "args": {"url_path": url_path, "data": data}}
            print("will not send data")

    def put(self, url_path: str, data: str):
        import requests

        if self.should_send:
            resp = requests.put(
                self.base_url + url_path, headers={"Content-Type": "Application/json"}, data=data.encode("utf8")
            )
            if not resp.ok:
                raise Exception(f"send data quality report failed(status={resp.status_code}): {resp.text}")
            print("data put to es done")
        else:
            self.data = {"method": "put", "args": {"url_path": url_path, "data": data}}
            print("will not send data")

    def delete_by_query(self, index: str, query: object):
        import requests

        data = json.dumps({"query": query})
        url_path = f"/{index}/_delete_by_query"
        if self.should_send:
            resp = requests.post(
                self.base_url + url_path, headers={"Content-Type": "Application/json"}, data=data.encode("utf8")
            )
            if not resp.ok:
                raise Exception(f"send data quality report failed(status={resp.status_code}): {resp.text}")
        else:
            self.data = {"method": "post", "args": {"url_path": url_path, "data": data}}
            print("will not send data")


class Reporter:
    def __init__(self, es_service: EsService, index_prefix: str = "", now: Any = None):
        self.es_service = es_service
        self.now = now
        self.index_prefix = index_prefix

    def _es_index_name(self, name: str):
        return f"{self.index_prefix}_{name}" if self.index_prefix else name

    def report_task_result(self, task_id: str, report: str):
        """
        es index:

        PUT /{index_prefix}_task_report
        {
          "mappings": {
            "properties": {
              "task_id": { "type": "wildcard" },
              "report": { "type": "text" },
              "created_at": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" }
            }
          }
        }
        """
        now = (self.now or datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S")
        data = {"task_id": task_id, "report": report, "created_at": now}
        self.es_service.post(f'/{self._es_index_name("task_report")}/_doc', json.dumps(data))
