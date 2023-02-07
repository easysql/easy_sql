import os
import subprocess
import time

from easy_sql.logger import logger


def _check_call(command: str) -> bool:
    logger.info(f"will exec command: {command}")
    try:
        return subprocess.check_call(["bash", "-c", command]) == 0
    except subprocess.CalledProcessError:
        return False


def _check_call_for_script(script_file: str) -> bool:
    logger.info(f"will exec script: {script_file}")
    try:
        return subprocess.check_call(["bash", script_file]) == 0
    except subprocess.CalledProcessError:
        return False


class FlinkTestClusterManager:
    def __init__(self, op_wait_secs: float = 3):
        import pyflink

        if not _check_call("type curl") or not _check_call("type grep"):
            raise Exception(
                "Can not find curl or grep. This module only works in a unix environment with curl and grep installed."
            )
        self.flink_home = os.path.dirname(pyflink.__file__)
        self.wait_secs = op_wait_secs

    def is_started(self):
        return _check_call("curl -s localhost:8081 | grep 'Apache Flink Web Dashboard'")

    def is_not_started(self):
        return _check_call("curl localhost:8081 2>&1 | grep 'Connection refused'")

    def start_cluster(self):
        success = _check_call_for_script(os.path.join(self.flink_home, "bin/start-cluster.sh"))
        if success:
            logger.info(f"Wait {self.wait_secs} for flink to be fully started.")
            time.sleep(self.wait_secs)
        else:
            raise Exception("Start flink cluster failed, please check the output.")

    def stop_cluster(self):
        success = _check_call_for_script(os.path.join(self.flink_home, "bin/stop-cluster.sh"))
        if success:
            logger.info(f"Wait {self.wait_secs} for flink to be fully stopped.")
            time.sleep(self.wait_secs)
        else:
            raise Exception("Stop flink cluster failed, please check the output.")
