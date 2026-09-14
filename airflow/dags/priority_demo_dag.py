"""Observe scheduling priority with an existing learning_pool of one slot.

Priority does not enforce execution order or preempt running tasks.
This DAG does not create or configure the pool.
"""

import logging
import time
from datetime import datetime

from airflow.sdk import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="priority_demo",
    start_date=datetime(2026, 9, 14),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def priority_demo():
    # Absolute weights avoid relying on the configured default weight rule.
    @task(pool="learning_pool", priority_weight=10, weight_rule="absolute")
    def high_priority():
        log.info("high_priority started")
        time.sleep(10)
        log.info("high_priority finished")

    @task(pool="learning_pool", priority_weight=2, weight_rule="absolute")
    def medium_priority():
        log.info("medium_priority started")
        time.sleep(10)
        log.info("medium_priority finished")

    @task(pool="learning_pool", priority_weight=1, weight_rule="absolute")
    def low_priority():
        log.info("low_priority started")
        time.sleep(10)
        log.info("low_priority finished")

    # Independent tasks: declaration order does not enforce execution order.
    high_priority()
    medium_priority()
    low_priority()


priority_demo()
