"""TaskFlow API: @dag / @task, return values passed as arguments, multiple_outputs."""
import datetime

from airflow.sdk import dag, task


@dag(start_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc), schedule=None, catchup=False)
def taskflow():
    @task
    def extract():
        return {"a": 1, "b": 2}

    @task(multiple_outputs=True)
    def split(d):
        return {"total": d["a"] + d["b"], "keys": sorted(d)}

    @task
    def load(total, keys):
        return f"{total}:{','.join(keys)}"

    s = split(extract())
    load(s["total"], s["keys"])


taskflow()
