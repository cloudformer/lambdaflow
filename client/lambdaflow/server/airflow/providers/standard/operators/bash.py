"""BashOperator (airflow.providers.standard.operators.bash): the last line of stdout is the XCom."""
import os
import subprocess

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.sdk import BaseOperator


class BashOperator(BaseOperator):
    EXTRA = ("bash_command", "env", "append_env", "cwd", "skip_on_exit_code")
    template_fields = ("bash_command", "env", "cwd")

    def __init__(self, task_id, bash_command, env=None, append_env=False, cwd=None, skip_on_exit_code=99, **kwargs):
        super().__init__(task_id, **kwargs)
        self.bash_command, self.env, self.append_env, self.cwd = bash_command, env, append_env, cwd
        self.skip_on_exit_code = skip_on_exit_code

    def execute(self, context):
        env = dict(os.environ, **(self.env or {})) if (self.env is None or self.append_env) else dict(self.env)
        r = subprocess.run(["bash", "-c", self.bash_command], env=env, cwd=self.cwd or "/tmp",
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(r.stdout, end="")
        if r.returncode == self.skip_on_exit_code:
            raise AirflowSkipException(f"Bash command returned exit code {r.returncode}")
        if r.returncode:
            raise AirflowException(f"Bash command failed. The command returned a non-zero exit code {r.returncode}.")
        lines = r.stdout.rstrip("\n").splitlines()
        return lines[-1] if lines else ""
