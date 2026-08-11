## Day 21｜Airflow Branching / Conditional Flow

### 今日目標

學習 Airflow 如何根據條件，決定 DAG 接下來要執行哪一條 Task 路徑。

---

### 1. Branching

一般固定流程：

```text
prepare
   ↓
process
   ↓
finish
```

Branching 可以建立條件流程：

```text
                 ┌→ process_data ──┐
check_data ──────┤                 ├→ finish
                 └→ skip_process ──┘
```

根據條件決定執行：

```text
process_data
```

或：

```text
skip_process
```

---

### 2. `@task.branch`

TaskFlow API 可以使用：

```python
@task.branch
```

建立 Branch Task。

例如：

```python
@task.branch
def check_data() -> str:
    has_data = True

    if has_data:
        return "process_data"

    return "skip_process"
```

`@task.branch` 的回傳值代表：

```text
downstream task_id
```

Airflow 會根據回傳的 `task_id` 決定接下來執行哪個 Task。

流程：

```text
check_data
    ↓
判斷條件
    ↓
回傳 downstream task_id
    ↓
Airflow 選擇 Branch
```

---

### 3. Branch Task 的執行狀態

假設：

```python
has_data = True
```

結果：

```text
check_data       success
process_data     success
skip_process     skipped
finish           success
```

如果：

```python
has_data = False
```

結果：

```text
check_data       success
process_data     skipped
skip_process     success
finish           success
```

核心：

```text
被選中的 Branch
→ 執行

沒有被選中的 Branch
→ skipped
```

注意：

```text
skipped ≠ failed
```

`skipped` 代表 Airflow 根據 Branch 結果**刻意不執行該 Task**，不是 Task 執行失敗。

---

### 4. Branch Join

當不同 Branch 最後需要重新匯合：

```text
process_data ──┐
               ├→ finish
skip_process ──┘
```

需要特別注意 `finish` 的 Trigger Rule。

因為 Branching 正常情況下，一定可能存在：

```text
success
+
skipped
```

---

### 5. 為什麼不能使用預設 `all_success`

Airflow 預設：

```text
trigger_rule = all_success
```

代表：

```text
所有直接 upstream
全部 success
↓
才執行 downstream
```

但是 Branching 可能產生：

```text
process_data = success
skip_process = skipped
```

因此不符合：

```text
all_success
```

---

### 6. `none_failed_min_one_success`

Branch Join 可以使用：

```python
@task(trigger_rule="none_failed_min_one_success")
def finish():
    print("Pipeline finished.")
```

意思：

```text
所有 upstream 都沒有 failed
+
至少一個 upstream success
↓
Task 可以執行
```

例如：

| process_data | skip_process | finish |
| ------------ | ------------ | ------ |
| success      | skipped      | 執行   |
| skipped      | success      | 執行   |
| success      | failed       | 不執行 |
| failed       | success      | 不執行 |
| skipped      | skipped      | 不執行 |

因此：

> `none_failed_min_one_success` 不是「只要一個成功就執行」。

而是：

```text
沒有 failed
+
至少一個 success
```

兩個條件都必須成立。

---

### 7. Demo DAG

本日新增：

```text
airflow/dags/branching_demo_dag.py
```

架構：

```python
@dag(...)
def branching_demo():

    @task.branch
    def check_data() -> str:
        has_data = True

        if has_data:
            return "process_data"

        return "skip_process"

    @task
    def process_data():
        print("Data exists. Processing data.")

    @task
    def skip_process():
        print("No data. Skip processing.")

    @task(trigger_rule="none_failed_min_one_success")
    def finish():
        print("Pipeline finished.")

    branch = check_data()

    process = process_data()
    skip = skip_process()

    branch >> [process, skip]

    [process, skip] >> finish()
```

---

## Day 21 核心整理

```text
@task.branch
      ↓
根據條件判斷
      ↓
回傳 downstream task_id
      ↓
Airflow 選擇執行路徑
      ↓
被選中的 Task → 執行
沒被選中的 Task → skipped
      ↓
不同 Branch 重新 Join
      ↓
需要考慮 Trigger Rule
```

### 今天學會

1. `@task.branch` 可以建立條件分支。
2. Branch Task 的結果會決定接下來執行哪個 downstream Task。
3. 被選中的 Task 會執行，沒有被選中的 Task 會變成 `skipped`。
4. `skipped` 與 `failed` 是不同的 Task State。
5. Branch 重新 Join 時需要考慮 Trigger Rule。
6. `none_failed_min_one_success` 代表「沒有 upstream failed，且至少一個 upstream success」。

**Day 21：Airflow Branching / Conditional Flow 完成。**
