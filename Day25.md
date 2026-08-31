# Day 25｜Airflow Failure Callback

## 今日主題

今天學習 Airflow 的 **Failure Callback**。

Day 24 已經學過：

- `execution_timeout`
- `retries`
- `retry_delay`

今天進一步處理：

> 當 Task Retry 多次之後仍然失敗，最終進入 Failed 狀態時，我們可以做什麼？

核心概念：

```text
Task
↓
Failure
↓
Retry
↓
仍然 Failure
↓
Retry 次數耗盡
↓
Final Failed
↓
on_failure_callback
↓
Logging / Alert
```

---

# 1. 為什麼需要 Failure Callback？

假設未來 TWSE Pipeline 每天自動執行：

```text
Airflow 啟動 Pipeline
↓
TWSE API 發生錯誤
↓
Task Failed
↓
Retry
↓
再次 Failed
↓
Retry 次數耗盡
↓
Final Failed
```

如果沒有其他機制，失敗資訊可能只停留在 Airflow UI。

實務上我們通常希望：

```text
Final Failed
↓
Failure Callback
↓
取得錯誤資訊
↓
Logging / Alert
```

未來甚至可以串接：

```text
Slack
Teams
Email
PagerDuty
```

通知維運人員。

---

# 2. `on_failure_callback` 是什麼？

Airflow Task 可以設定：

```python
on_failure_callback
```

指定 Task 最終失敗時要執行的 Callback Function。

例如：

```python
def task_failure_callback(context):
    print("Task failed!")
```

Task 設定：

```python
@task(
    on_failure_callback=task_failure_callback
)
def process():
    ...
```

概念：

```text
process Task
↓
Final Failed
↓
Airflow 呼叫
task_failure_callback()
```

因此：

> `on_failure_callback` 可以用來定義 Task 最終失敗後的處理行為。

---

# 3. Callback 是什麼？

Callback 可以理解為：

> 當特定事件發生時，由 Airflow 自動呼叫的 Function。

今天的事件就是：

```text
Task Final Failed
```

所以：

```text
Task Failed
↓
on_failure_callback
↓
執行指定 Function
```

例如：

```python
def task_failure_callback(context):
    print("Task failed!")
```

---

# 4. Callback 本身不是 Airflow Task

這是今天的重要觀念。

Callback：

```text
不是 DAG 裡另外一個 Task
```

而是：

```text
Airflow Task
│
├── 執行 Business Logic
│
└── 最終 Failed
       ↓
Airflow 呼叫 Callback Function
```

所以：

```text
Task
≠
Callback
```

如果 DAG 是：

```text
start
↓
failing_task
```

Failure Callback 不會變成：

```text
start
↓
failing_task
↓
callback_task
```

因為 Callback 並不是 DAG Dependency 中的另一個 Task。

---

# 5. Callback Context

Airflow 呼叫 Callback 時，會提供：

```python
context
```

例如：

```python
def task_failure_callback(context):
    ...
```

`context` 包含這次 Airflow 執行的相關資訊。

今天實際關注：

```text
dag_id
task_id
run_id
exception
```

因此 Failure Callback 可以知道：

```text
哪個 DAG 失敗？
哪個 Task 失敗？
哪一次 DAG Run？
發生什麼錯誤？
```

---

# 6. 取得 Task 資訊

例如：

```python
def task_failure_callback(context):

    task_instance = context["task_instance"]
    exception = context.get("exception")

    print("=== FAILURE CALLBACK ===")
    print(f"DAG ID: {task_instance.dag_id}")
    print(f"Task ID: {task_instance.task_id}")
    print(f"Run ID: {task_instance.run_id}")
    print(f"Exception: {exception}")
```

可能得到：

```text
DAG ID:
failure_callback_demo

Task ID:
failing_task

Run ID:
manual__...

Exception:
Demo task failure
```

這些資訊未來就可以拿去組成告警訊息。

---

# 7. 為什麼需要 `run_id`？

同一個 DAG 會執行很多次。

例如：

```text
twse_daily_pipeline

2026-08-29 Run
2026-08-30 Run
2026-08-31 Run
```

只知道：

```text
DAG ID
Task ID
```

還不能完全定位是哪一次執行發生問題。

因此還需要：

```text
run_id
```

協助識別：

> 到底是哪一次 DAG Run 發生錯誤。

概念：

```text
DAG ID
→ 哪個 Pipeline

Task ID
→ Pipeline 裡哪個 Task

Run ID
→ 哪一次執行

Exception
→ 發生什麼問題
```

---

# 8. Failure Callback 與 Retry

今天實際設定：

```python
retries=2
```

代表：

```text
第一次執行
+
最多 Retry 2 次
=
最多 3 Attempts
```

實際流程：

```text
Attempt 1
↓
Failed
↓
還有 Retry
↓
Retry

Attempt 2
↓
Failed
↓
還有 Retry
↓
Retry

Attempt 3
↓
Failed
↓
Retry 已耗盡
↓
Final Failed
↓
on_failure_callback
```

---

# 9. 今天實際觀察到的 Callback 時機

今天實際驗證：

```text
retries=2
```

Callback 並不是：

```text
Attempt 1 Failed
→ Callback

Attempt 2 Failed
→ Callback

Attempt 3 Failed
→ Callback
```

而是在 Retry 次數耗盡、Task 最終進入 Failed 時：

```text
Attempt 1
→ Failed
→ Retry

Attempt 2
→ Failed
→ Retry

Attempt 3
→ Failed
→ Final Failed
→ on_failure_callback
```

因此今天的核心理解是：

> 在本次實作中，`on_failure_callback` 是在最後一次 Attempt 仍然失敗、Task 最終進入 Failed 狀態時被觸發。

---

# 10. `retries=2` 再複習

```python
retries=2
```

不是總共執行兩次。

而是：

```text
Initial Attempt
+
2 Retries
=
最多 3 Attempts
```

也就是：

```text
Attempt 1
↓ Failed

Attempt 2（Retry 1）
↓ Failed

Attempt 3（Retry 2）
↓ Failed

Final Failed
```

最後才進入今天關注的：

```text
on_failure_callback
```

---

# 11. 今天的 Demo DAG

新增：

```text
airflow/dags/failure_callback_demo_dag.py
```

DAG：

```text
failure_callback_demo
```

Dependency：

```text
start
↓
failing_task
```

其中 `failing_task` 故意：

```python
raise ValueError("Demo task failure")
```

讓 Task 一定發生 Failure。

設定：

```python
@task(
    retries=2,
    retry_delay=timedelta(seconds=3),
    on_failure_callback=task_failure_callback,
)
def failing_task():

    print("Processing data...")

    raise ValueError("Demo task failure")
```

藉此觀察：

```text
Failure
↓
Retry
↓
Failure
↓
Retry
↓
Final Failure
↓
Callback
```

---

# 12. Failure Callback 適合做什麼？

Callback 很適合做：

```text
Logging
Alert
Notification
Failure Metadata 紀錄
```

例如未來：

```text
Task Final Failed
↓
Failure Callback
├── Slack
├── Teams
├── Email
└── Monitoring System
```

通知內容可以包含：

```text
Pipeline Failed

DAG ID:
twse_daily_pipeline

Task ID:
run_twse_pipeline

Run ID:
scheduled__...

Exception:
TWSE API connection timeout
```

---

# 13. Failure Callback 不應該做什麼？

不要把主要 ETL Business Logic 塞進 Callback。

例如不建議：

```text
Task Failed
↓
Callback
↓
重新執行整個 ETL
↓
大量修改 Database
↓
執行另一套 Business Logic
```

因為不同責任應該分開：

```text
ETL Execution
→ Airflow Task

重新嘗試
→ Retry

Failure Handling
→ Callback
```

這樣 Pipeline Architecture 才會清楚。

---

# 14. Day 24 + Day 25 串聯

Day 24：

```text
execution_timeout
retries
retry_delay
```

Day 25：

```text
on_failure_callback
```

現在可以組成：

```text
Task
↓
開始 Attempt
↓
執行成功？
├── Yes
│    ↓
│  Success
│
└── No
     ↓
   Failure
     ↓
還有 Retry？
├── Yes
│    ↓
│ retry_delay
│    ↓
│ New Attempt
│
└── No
     ↓
 Final Failed
     ↓
on_failure_callback
     ↓
Logging / Alert
```

---

# 15. `execution_timeout` + Retry + Callback

未來一個比較完整的 Task 可以具有：

```python
@task(
    execution_timeout=timedelta(minutes=5),
    retries=2,
    retry_delay=timedelta(minutes=1),
    on_failure_callback=task_failure_callback,
)
def process():
    ...
```

各自負責：

### `execution_timeout`

```text
單次 Attempt 最多可以跑多久？
```

### `retries`

```text
Failure 後最多額外重試幾次？
```

### `retry_delay`

```text
Retry 前等待多久？
```

### `on_failure_callback`

```text
Task 最終失敗後要做什麼？
```

---

# 16. 套用到 TWSE Pipeline

未來正式 TWSE Pipeline 可以形成：

```text
TWSE API
↓
Airflow Task
│
├── HTTP Timeout
│
├── execution_timeout
│
├── retries
│
├── retry_delay
│
└── on_failure_callback
       ↓
   Failure Alert
```

這代表 Pipeline 不只是：

```text
把資料跑完
```

而開始具備：

```text
異常控制
重試機制
執行時間控制
失敗後處理
可觀測性
```

這些都是 Production Data Pipeline 很重要的能力。

---

# 17. 今日實際學會

## 1. `on_failure_callback` 是拿來做什麼？

當 Task Retry 多次後仍然失敗，最終進入 Failed 狀態時，可以呼叫指定的 Callback Function。

例如：

```text
Final Failed
↓
Logging
Alert
Notification
```

---

## 2. Callback 的 `context` 可以取得什麼？

今天學到可以取得：

```text
dag_id
task_id
run_id
exception
```

用來定位：

```text
哪個 DAG
哪個 Task
哪次 Run
發生什麼錯誤
```

---

## 3. Callback 本身是不是 Airflow Task？

不是。

```text
Callback
≠
Airflow Task
```

它是特定事件發生時，由 Airflow 呼叫的 Function。

---

## 4. `retries=2` 時 Callback 在哪一次 Attempt 被觸發？

今天實際觀察：

```text
Attempt 1
→ Failed
→ Retry

Attempt 2
→ Failed
→ Retry

Attempt 3
→ Failed
→ Final Failed
→ Failure Callback
```

也就是：

> Retry 次數耗盡，最後一次 Attempt 仍然失敗時觸發。

---

# Day 25 核心整理

今天最重要的流程：

```text
Attempt 1
↓
Failed
↓
Retry

Attempt 2
↓
Failed
↓
Retry

Attempt 3
↓
Failed
↓
Retry Exhausted
↓
Final Failed
↓
on_failure_callback
↓
取得 Context
↓
DAG ID
Task ID
Run ID
Exception
↓
Logging / Alert
```

四個核心概念：

```text
execution_timeout
→ 單次 Attempt 最多跑多久

retries
→ Failure 後最多額外再試幾次

retry_delay
→ Retry 前等待多久

on_failure_callback
→ Task 最終失敗後執行失敗處理
```

## Day 25 完成

**Airflow Failure Callback / Failure Handling ✅**
