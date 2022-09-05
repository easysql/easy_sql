from datetime import datetime
from typing import Dict, List, Optional

from .step import ReportCollector, Step


class StepStatus:
    NOT_STARTED = "NOT_STARTED"
    SKIPPED = "SKIPPED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"


class StepReport:
    def __init__(self, step: Step):
        self.step = step
        self.status = StepStatus.NOT_STARTED
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.execution_time = 0
        self.messages = []

    def update(self, status: Optional[str] = None, message: Optional[str] = None) -> "StepReport":
        if status is not None:
            if status == StepStatus.RUNNING:
                self.start_time = datetime.now()
            if status in [StepStatus.FAILED, StepStatus.SUCCEEDED]:
                self.end_time = datetime.now()
                start_time = self.start_time or datetime.now()
                self.execution_time = (datetime.now() - start_time).total_seconds()
            self.status = status
        if message is not None and message != "":
            self.messages.append(message)
        return self

    def report_as_text(self, total_execution_time: float, verbose: bool = False):
        start_time_str = self.start_time.strftime("%Y-%m-%d %H:%M:%S") if self.start_time is not None else ""
        end_time_str = self.end_time.strftime("%Y-%m-%d %H:%M:%S") if self.end_time is not None else ""
        verbose_parts = "\n" + f"sql: {self.step.select_sql}" if verbose else ""
        return f"""
===================== REPORT FOR {self.step.id} ==================
config: {self.step.target_config} {verbose_parts}
status: {self.status}
start time: {start_time_str}, end time: {end_time_str}, execution time: {self.execution_time}s - {self.execution_time / total_execution_time * 100:.2f}%
messages:
""" + "\n".join(
            self.messages
        )


class SqlProcessorReporter(ReportCollector):
    def __init__(
        self,
        report_task_id: str,
        report_hdfs_path: Optional[str] = None,
        report_es_url: Optional[str] = None,
        report_es_index_prefix: Optional[str] = None,
    ):
        self.report_task_id = report_task_id
        self.report_hdfs_path = report_hdfs_path
        self.report_es_url = report_es_url
        self.report_es_index_prefix = report_es_index_prefix
        self._steps = None
        self.step_reports: Optional[Dict[str, StepReport]] = None
        self.start_time = datetime.now()

    def init(self, steps: List[Step]):
        self._steps = steps
        self.step_reports = {step.id: StepReport(step) for step in steps}

    def collect_report(self, step: Step, status: Optional[str] = None, message: Optional[str] = None):
        assert self.step_reports is not None
        report = self.step_reports[step.id]
        if status is not None:
            report.update(status=status)
        if message is not None:
            report.update(message=message)

    def print_report(self, verbose: bool = False):
        report = self.get_report(verbose)
        self._report_to_stdout(report)
        if self.report_hdfs_path is not None:
            self._report_to_hdfs(report)
        if self.report_es_url is not None:
            self._report_to_es(report)

    def _report_to_es(self, report):
        from easy_sql.report import EsService, Reporter

        assert self.report_es_url is not None
        assert self.report_es_index_prefix is not None
        Reporter(EsService(self.report_es_url), index_prefix=self.report_es_index_prefix).report_task_result(
            task_id=self.report_task_id, report=report
        )

    def _report_to_stdout(self, report):
        print(report)

    def _report_to_hdfs(self, report):
        import hashlib
        import os

        assert self.report_hdfs_path is not None
        file_name = os.path.basename(self.report_hdfs_path)
        md5 = hashlib.md5()
        md5.update(file_name.encode("utf8"))
        folder = md5.hexdigest()[:2]
        local_tmp_folder = f"/tmp/sql_processor_reports/{folder}/"
        local_tmp_file = f"{local_tmp_folder}/{file_name}"

        os.makedirs(local_tmp_folder, exist_ok=True)
        with open(local_tmp_file, "w") as f:
            f.write(report)

        import subprocess

        subprocess.run(
            ["bash", "-c", f"hdfs dfs -rm {self.report_hdfs_path} || exit 0"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        result = subprocess.run(
            [
                "bash",
                "-c",
                (
                    f"hdfs dfs -mkdir -p {os.path.basename(self.report_hdfs_path)} && "
                    f"hdfs dfs -put {local_tmp_file} {self.report_hdfs_path}"
                ),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if result.returncode != 0:
            raise Exception(f"upload file {local_tmp_file} to hdfs failed, result code: {result.returncode}")

    def get_report(self, verbose: bool = False) -> str:
        report = []
        total_execution_seconds = (datetime.now() - self.start_time).total_seconds()
        assert self._steps is not None
        assert self.step_reports is not None
        for step in self._steps:
            report.append(self.step_reports[step.id].report_as_text(total_execution_seconds, verbose))
        report.append(f"\ntotal execution time: {total_execution_seconds}s")
        return "\n".join(report)
