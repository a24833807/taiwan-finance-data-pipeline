# Day 23｜Airflow TaskGroup

## 今日目標

學習使用 Airflow `TaskGroup` 將邏輯上相關的 Tasks 進行分組，改善 DAG 結構與 Airflow Graph 的可讀性。

核心概念：

```text
TaskGroup
→ 組織 Tasks
→ 改善 DAG 可讀性
→ 本身不是執行單位
```

---

## 1. 為什麼需要 TaskGroup？

當 DAG 很簡單時：

```text
prepare
   ↓
process
   ↓
finish
```

不需要特別進行分組。

但實際 Pipeline 可能逐漸變成：

```text
check_config
     ↓
check_source
     ↓
extract_stock
     ↓
extract_index
     ↓
transform_stock
     ↓
validate_stock
     ↓
load_stock
     ↓
send_summary
```

當 Task 數量增加後，Airflow Graph 會越來越難閱讀。

這時可以透過 `TaskGroup` 將邏輯上相關的 Tasks 整理在一起：

```text
prepare
   ↓
┌──── processing_group ────┐
│                          │
│ transform → validate     │
│                          │
└──────────────────────────┘
   ↓
finish
```

因此：

> TaskGroup 主要解決的是 DAG 組織與可讀性的問題。

---

# 2. TaskGroup 是什麼？

TaskGroup 可以將邏輯上相關的 Tasks 組成一個群組。

例如：

```text
processing_group
│
├── transform
│      ↓
└── validate
```

使用 TaskFlow API：

```python
@task_group(group_id="processing_group")
def processing_group():
    ...
```

但需要注意：

```text
TaskGroup
≠ Task
≠ DAG
```

TaskGroup 本身不會被 Scheduler 當成一個真正的 Task 執行。

真正執行的仍然是：

```text
transform
validate
```

TaskGroup 只是將它們組織起來。

---

# 3. TaskGroup 的架構

今天建立的 DAG：

```text
task_group_demo
```

完整架構：

```text
prepare
   ↓
┌──── processing_group ────┐
│                          │
│ transform                │
│     ↓                    │
│ validate                 │
│                          │
└──────────────────────────┘
   ↓
finish
```

實際執行順序仍然是：

```text
prepare
   ↓
transform
   ↓
validate
   ↓
finish
```

不是：

```text
prepare
   ↓
TaskGroup 執行
   ↓
finish
```

---

# 4. 本日 Demo

新增：

```text
airflow/dags/task_group_demo_dag.py
```

範例：

```python
from datetime import datetime

from airflow.sdk import dag, task, task_group


@dag(
    dag_id="task_group_demo",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def task_group_demo():

    @task
    def prepare():
        print("Prepare data.")

    @task_group(group_id="processing_group")
    def processing_group():

        @task
        def transform():
            print("Transform data.")

        @task
        def validate():
            print("Validate data.")

        transform_task = transform()
        validate_task = validate()

        transform_task >> validate_task

    @task
    def finish():
        print("Pipeline finished.")

    prepare_task = prepare()
    processing = processing_group()
    finish_task = finish()

    prepare_task >> processing >> finish_task


task_group_demo()
```

---

# 5. `@task_group`

使用：

```python
@task_group(group_id="processing_group")
```

建立：

```text
processing_group
```

並將相關 Tasks 定義在 Group 中：

```python
@task_group(group_id="processing_group")
def processing_group():

    @task
    def transform():
        ...

    @task
    def validate():
        ...
```

Airflow UI 就可以將：

```text
transform
validate
```

整理在：

```text
processing_group
```

底下。

---

# 6. TaskGroup 與 Task ID

TaskGroup 預設會將 `group_id` 加到 Group 內 Task ID 前面。

例如：

```text
group_id:
processing_group

task_id:
transform
```

完整 Task ID 會變成：

```text
processing_group.transform
```

另外：

```text
processing_group.validate
```

因此規則可以理解成：

```text
group_id.task_id
```

---

# 7. 為什麼 Task ID 要加 Group Prefix？

假設 DAG 未來有兩個 Group：

```text
stock_processing
└── validate

index_processing
└── validate
```

兩個 Group 都存在：

```text
validate
```

如果只有：

```text
validate
```

就難以區分。

加入 Group Prefix 後：

```text
stock_processing.validate

index_processing.validate
```

就可以清楚知道 Task 屬於哪個 Group。

因此 TaskGroup 可以形成類似：

```text
Namespace
```

的效果。

好處：

```text
降低 Task ID 衝突
+
增加 Task 所屬 Group 的辨識度
```

---

# 8. TaskGroup 不會改變 Dependency

Task 放進 TaskGroup 後，原本的 Dependency 邏輯仍然存在。

例如：

```text
prepare
   ↓
processing_group
│
├── transform
│      ↓
└── validate
       ↓
finish
```

真正的 Dependency：

```text
prepare
   ↓
processing_group.transform
   ↓
processing_group.validate
   ↓
finish
```

TaskGroup 只是整理結構。

---

# 9. Group 裡 Task Failed 會怎樣？

例如：

```text
prepare                         success
processing_group.transform      success
processing_group.validate       failed
finish                          upstream_failed
```

即使 `validate` 位於 TaskGroup 裡：

```text
validate
→ failed
```

仍然會影響 downstream：

```text
finish
```

如果 `finish` 使用預設：

```text
trigger_rule = all_success
```

則 upstream 發生 Failed：

```text
finish
→ upstream_failed
```

因此：

> TaskGroup 不會把 Group 裡面的 Task 隔離起來。

原本的：

```text
Dependency
Trigger Rule
Task State
```

都仍然有效。

---

# 10. TaskGroup 不是 SubDAG

TaskGroup 不代表：

```text
DAG
↓
另一個 DAG
```

而是：

```text
同一個 DAG
│
├── prepare
│
├── processing_group
│      ├── transform
│      └── validate
│
└── finish
```

所有 Tasks 仍然屬於同一個 DAG。

TaskGroup 只是其中一種組織方式。

---

# 11. 什麼時候適合使用 TaskGroup？

適合：

```text
DAG Tasks 數量增加
        ↓
出現不同邏輯區塊
        ↓
Graph 開始難閱讀
        ↓
需要整理 Tasks
        ↓
使用 TaskGroup
```

例如：

```text
TWSE Pipeline
│
├── Pre-check
│     ├── check_date
│     └── check_source
│
├── Processing
│     ├── extract
│     ├── transform
│     └── validate
│
└── Loading
      ├── load
      └── verify
```

這時 TaskGroup 可以讓 DAG 結構更加清楚。

---

# 12. 什麼時候不需要 TaskGroup？

如果 DAG 只有：

```text
Task A
  ↓
Task B
  ↓
Task C
```

本身已經非常容易理解，就沒有必要為了使用 TaskGroup 而增加結構。

核心原則：

```text
不是：

學會 TaskGroup
      ↓
一定要使用


而是：

DAG 出現組織問題
      ↓
TaskGroup 適合解決
      ↓
才使用
```

---

# 13. 為什麼目前不修改正式 TWSE Pipeline？

目前正式 Pipeline：

```text
Airflow Task
      ↓
run_pipeline(trade_date)
      ↓
Python ETL
│
├── Extract
├── Transform
├── Validate
└── Load
```

目前 Airflow 層級主要還是一個 Task。

因此並不存在：

```text
Airflow Tasks 太多
Graph 太複雜
需要分組
```

的問題。

所以目前沒有必要為了 TaskGroup，刻意將正式 ETL 拆成多個 Airflow Tasks。

是否使用 TaskGroup：

> 應根據實際 DAG 複雜度與專案需求決定，而不是為了展示技術而使用。

---

# 14. Day 22 今天學會

1. TaskGroup 可以把**邏輯上相關的 Tasks** 組成群組，改善 DAG 結構與 Graph 可讀性。

2. TaskGroup 本身不是 Task，真正執行的仍然是 Group 裡面的 Tasks。

3. TaskGroup 預設會將 `group_id` 加到 Task ID 前面。

例如：

```text
processing_group.transform
processing_group.validate
```

4. `group_id.task_id` 可以形成 Namespace，降低不同 Group 之間 Task ID 衝突，也能清楚表示 Task 所屬 Group。

5. TaskGroup 不會改變原本的 Task Dependency、Trigger Rule 與 Task State。

6. Group 裡的 Task 如果 Failed，仍然會影響 downstream Task。

7. 是否使用 TaskGroup 應依照實際專案與 DAG 複雜度決定。

8. 不應為了使用某項 Airflow 功能，而刻意增加 Pipeline 架構複雜度。

---

# Day 22 核心整理

```text
TaskGroup
    ↓
將邏輯相關 Tasks 分組
    ↓
改善 DAG Graph 可讀性
    ↓
TaskGroup 本身不執行
    ↓
真正執行的是 Group 裡的 Tasks
```

Task ID：

```text
group_id.task_id

例如：

processing_group.transform
processing_group.validate
```

Dependency：

```text
Task 放進 TaskGroup
≠
Dependency 被隔離

Group 內 Task Failed
→ 仍然可能影響 downstream
```

使用原則：

```text
有實際 DAG 組織問題
        ↓
選擇 TaskGroup 解決

而不是

學會 TaskGroup
        ↓
為了展示技術硬塞進 Pipeline
```

**Day 23：Airflow TaskGroup 完成。**
