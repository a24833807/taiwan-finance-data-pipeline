# Day 31｜Airflow Dynamic Task Mapping（動態任務映射）

## 1. 今日主題

今天學習：

> **Dynamic Task Mapping（動態任務映射）**：讓 Airflow 根據 Runtime（執行期間）取得的輸入資料，動態展開多個 Mapped Task Instances（映射任務實例）。

核心語法：

```python
process_date.expand(
    trade_date=trade_dates
)
```

今天最重要的觀念：

> **Python `for` loop 是在一個 Task Instance 裡處理多筆資料；Dynamic Task Mapping 則讓 Airflow 根據輸入資料建立並分別管理多個 Mapped Task Instances。**

---

# 2. 為什麼需要 Dynamic Task Mapping？

假設需要處理：

```text
20260901
20260902
20260903
```

如果使用一般 Python：

```python
for trade_date in trade_dates:
    process(trade_date)
```

概念上是：

```text
一個 Task Instance
        │
        ├── 20260901
        ├── 20260902
        └── 20260903
```

日期的迴圈是在同一個 Python Task 執行過程中完成。

Airflow 不會因為 Python `for` loop，就自動把每個日期當成獨立的 Mapped Task Instance 管理。

---

# 3. Dynamic Task Mapping 的做法

先建立取得日期的 Task：

```python
@task
def get_trade_dates() -> list[str]:
    return [
        "20260901",
        "20260902",
        "20260903",
    ]
```

再建立處理單一日期的 Task：

```python
@task
def process_date(trade_date: str) -> None:
    print(f"Processing: {trade_date}")
```

取得：

```python
trade_dates = get_trade_dates()
```

然後：

```python
process_date.expand(
    trade_date=trade_dates
)
```

Airflow 會根據輸入集合進行 Dynamic Task Mapping。

概念：

```text
get_trade_dates
        │
        ↓
[
  "20260901",
  "20260902",
  "20260903"
]
        │
        ↓
     expand()
        │
        ├── process_date[0]
        ├── process_date[1]
        └── process_date[2]
```

---

# 4. `expand()` 是什麼？

今天最重要的新語法：

```python
process_date.expand(
    trade_date=trade_dates
)
```

`expand()` 的作用是：

> **根據輸入集合的內容，動態展開 Mapped Task Instances。**

例如：

```python
trade_dates = [
    "20260901",
    "20260902",
    "20260903",
]
```

有三個元素。

因此：

```text
3 個輸入元素
       ↓
    expand()
       ↓
3 個 Mapped Task Instances
```

結果概念上：

```text
process_date[0]
→ 20260901

process_date[1]
→ 20260902

process_date[2]
→ 20260903
```

---

# 5. 不是因為「日期」才可以 Mapping

Dynamic Task Mapping 並不是只能處理日期。

例如：

```python
stock_ids = [
    "2330",
    "2317",
    "2454",
    "2881",
]
```

如果：

```python
process_stock.expand(
    stock_id=stock_ids
)
```

就可以展開：

```text
process_stock[0] → 2330
process_stock[1] → 2317
process_stock[2] → 2454
process_stock[3] → 2881
```

所以真正的概念是：

> `expand()` 根據輸入集合展開工作，而不是因為資料是日期。

---

# 6. Mapped Task Instance 是什麼？

假設：

```python
process_date.expand(
    trade_date=[
        "20260901",
        "20260902",
        "20260903",
    ]
)
```

Airflow 會分別管理：

```text
process_date[0]
process_date[1]
process_date[2]
```

這些就是不同的：

> **Mapped Task Instances（映射任務實例）**

它們屬於同一個：

```text
task_id = process_date
```

但是是不同的 Mapping Instance。

因此 Airflow 可以分別追蹤它們的：

- State（狀態）
- Log（日誌）
- Retry（重試）
- Failure（失敗）
- 執行時間

---

# 7. `map_index` 是什麼？

例如：

```text
process_date[0]
process_date[1]
process_date[2]
```

其中：

```text
[0]
[1]
[2]
```

就是不同的 Mapping Index。

Airflow 可以使用：

```text
map_index
```

來區分同一個 Mapped Task 中不同的 Task Instance。

例如：

```text
task_id = process_date
map_index = 0
```

對應：

```text
20260901
```

---

```text
task_id = process_date
map_index = 1
```

對應：

```text
20260902
```

---

```text
task_id = process_date
map_index = 2
```

對應：

```text
20260903
```

因此可以記：

```text
task_id
→ 告訴你是哪個 Task

map_index
→ 告訴你是 Mapping 裡的哪一份工作
```

---

# 8. Python `for` loop vs Dynamic Task Mapping

這是 Day 31 最重要的比較。

## Python `for` loop

例如：

```python
@task
def process_dates():
    for trade_date in trade_dates:
        process(trade_date)
```

概念：

```text
process_dates
     │
     ├── 20260901
     ├── 20260902
     └── 20260903
```

主要仍是一個 Task Instance。

---

## Dynamic Task Mapping

例如：

```python
process_date.expand(
    trade_date=trade_dates
)
```

概念：

```text
process_date
     │
     ├── [0] → 20260901
     ├── [1] → 20260902
     └── [2] → 20260903
```

Airflow 可以分別管理：

```text
process_date[0]
process_date[1]
process_date[2]
```

---

# 9. 最大差異

Python `for` loop：

```text
一個 Task Instance
↓
自己處理多筆資料
```

Dynamic Task Mapping：

```text
一份輸入
↓
一個 Mapped Task Instance
```

因此：

```text
Python for loop
→ Python 自己管理迴圈

Dynamic Task Mapping
→ Airflow 管理每個 Mapping Instance
```

這是今天最核心的差別。

---

# 10. Failure Demo

為了測試不同 Mapped Task Instances 是否可以分開管理，在：

```python
process_date()
```

加入：

```python
@task
def process_date(trade_date: str) -> None:
    print(f"Processing: {trade_date}")

    if trade_date == "20260902":
        raise ValueError("Day 31 mapping failure demo")
```

仍然使用：

```python
process_date.expand(
    trade_date=trade_dates
)
```

預期結果：

```text
process_date[0]
20260901
→ Success

process_date[1]
20260902
→ Failed

process_date[2]
20260903
→ Success
```

---

# 11. 一個 Mapping Failure 不代表全部 Failure

如果：

```text
20260902
```

發生 Failure：

```text
process_date[1]
→ Failed
```

不代表：

```text
process_date[0]
process_date[2]
```

一定一起失敗。

因為它們是不同的 Mapped Task Instances。

例如：

```text
process_date[0]
20260901
Success ✅

process_date[1]
20260902
Failed ❌

process_date[2]
20260903
Success ✅
```

這就是 Dynamic Task Mapping 的重要價值：

> **Airflow 可以分別追蹤每一份映射工作的執行狀態。**

---

# 12. `get_trade_dates()` 的輸出

程式：

```python
trade_dates = get_trade_dates()
```

在 DAG 定義時，不應理解成：

```text
Python 現在立刻取得 list[str]
```

而應理解成：

```text
get_trade_dates
       ↓
Task 執行
       ↓
產生 Output
       ↓
透過 Airflow 傳給下游 Mapping
```

因此不能寫成：

```python
trade_dates = get_trade_dates()

for trade_date in trade_dates:
    ...
```

把它當普通 Python List 使用。

---

# 13. 與 XCom 的關係

Day 19 學過：

> XCom 適合傳遞小型 Metadata（中繼資料），不適合大型 DataFrame。

今天：

```python
get_trade_dates()
```

回傳：

```python
[
    "20260901",
    "20260902",
    "20260903",
]
```

這種小型日期 List 屬於合理的小型資料。

概念：

```text
get_trade_dates
       ↓
小型日期 List
       ↓
Airflow Task Output / XCom
       ↓
expand()
       ↓
Mapped Task Instances
```

但是不應該：

```text
Extract
↓
巨大 DataFrame
↓
XCom
↓
expand()
```

大型資料仍然應考慮：

```text
S3 / Object Storage / File / Database
```

然後 XCom 傳：

```text
URI
Object Key
File Path
Table Name
Partition
```

等 Metadata。

---

# 14. 與 Taiwan Finance Data Pipeline 的關係

目前正式 TWSE Pipeline 的 Range Mode：

```bash
python src/main.py \
  --start-date 20260901 \
  --end-date 20260905
```

概念：

```text
一個 Python Process
       ↓
for date in dates
       ↓
20260901
20260902
20260903
20260904
20260905
```

如果未來使用 Dynamic Task Mapping：

```text
Trade Dates
     ↓
  expand()
     ↓

20260901 → Mapped Task Instance
20260902 → Mapped Task Instance
20260903 → Mapped Task Instance
20260904 → Mapped Task Instance
20260905 → Mapped Task Instance
```

Airflow 就能分別管理每個日期。

不過 Day 31：

> **不修改正式 TWSE Pipeline。**

目前先用 Demo 學習 Dynamic Task Mapping。

---

# 15. 與 Day 28 Task Boundary 的關係

Day 28 學過：

> Task Boundary（Task 邊界）決定 Airflow 在哪裡分開管理執行、State、Retry、Timeout、Monitoring 與 Failure。

Dynamic Task Mapping：

```text
20260901
20260902
20260903
```

可以形成：

```text
process_date[0]
process_date[1]
process_date[2]
```

因此 Airflow 可以知道：

```text
20260901 → Success
20260902 → Failed
20260903 → Success
```

而不是只有：

```text
process_dates
→ Failed
```

這提高了 Task-level Observability（任務層級可觀測性）。

---

# 16. 與 Day 29 Pool 的關係

假設未來：

```text
Dynamic Task Mapping
↓
產生 100 個 Load Mapped Task Instances
```

不代表：

```text
100 個 Task
↓
全部同時打 PostgreSQL
```

因為可能造成：

- DB Connection 過多
- Query / Transaction 過多
- I/O 壓力
- PostgreSQL 負載過高

因此可以搭配：

```text
postgres_pool
Slots = 3
```

形成：

```text
100 個 Mapped Task Instances
             ↓
        postgres_pool
          Slots = 3
             ↓
         PostgreSQL
```

所以：

```text
Dynamic Task Mapping
→ 動態產生工作

Pool
→ 控制這些工作同時能使用多少有限資源
```

可以記：

> **Mapping 管工作數量，Pool 管併發容量。**

---

# 17. 與 Day 30 Priority 的關係

如果大量 Mapped Tasks 同時等待有限資源，還可能搭配：

```text
Priority Weight
```

所以目前可以串成：

```text
Dynamic Task Mapping
→ 有多少工作？

Dependency
→ 誰必須等誰？

Pool
→ 同時允許多少工作使用有限資源？

Priority
→ 多個工作競爭有限資源時誰比較優先？
```

---

# 18. Day 28～31 串聯

目前四天的概念：

```text
Day 28
Task Boundary
→ 工作應該怎麼切？

Day 29
Pool & Concurrency
→ 同時最多允許多少工作使用有限資源？

Day 30
Priority Weight
→ 競爭有限資源時誰比較優先？

Day 31
Dynamic Task Mapping
→ 根據 Runtime 輸入動態展開多少份工作？
```

組合：

```text
Runtime Input
      ↓
Dynamic Task Mapping
      ↓
Mapped Task Instances
      ↓
Dependency
      ↓
Ready Tasks
      ↓
Pool
      ↓
Priority
      ↓
Executor / Worker
```

這是概念模型，用來理解各功能負責的問題，不代表 Airflow Scheduler 內部完整實作流程。

---

# 19. Day 31 實作檔案

今天新增：

```text
airflow/dags/dynamic_task_mapping_demo_dag.py
```

沒有修改正式 TWSE Pipeline。

沒有修改：

```text
src/*
docker-compose.yml
docker-compose.airflow.yml
requirements.txt
.env
其他 Demo DAG
```

---

# 20. Day 31 Demo Code

```python
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="dynamic_task_mapping_demo",
    start_date=datetime(2026, 9, 15),
    schedule=None,
    catchup=False,
    tags=["learning"],
)
def dynamic_task_mapping_demo():

    @task
    def get_trade_dates() -> list[str]:
        return [
            "20260901",
            "20260902",
            "20260903",
        ]

    @task
    def process_date(trade_date: str) -> None:
        print(f"Processing: {trade_date}")

    trade_dates = get_trade_dates()

    process_date.expand(
        trade_date=trade_dates
    )


dynamic_task_mapping_demo()
```

---

# 21. Failure Demo Code

確認第一階段三個 Mapping 都成功後，可以修改：

```python
@task
def process_date(trade_date: str) -> None:
    print(f"Processing: {trade_date}")

    if trade_date == "20260902":
        raise ValueError("Day 31 mapping failure demo")
```

預期：

```text
[0] 20260901 → Success
[1] 20260902 → Failed
[2] 20260903 → Success
```

---

# 22. 今日理解題

## Q1：`expand()` 是做什麼的？

> 根據輸入集合，在 Runtime 動態展開多個 Mapped Task Instances。

例如：

```text
3 個輸入元素
↓
expand()
↓
3 個 Mapped Task Instances
```

---

## Q2：為什麼三個日期產生三個 Mapped Task Instances？

因為：

> 傳給 `expand()` 的輸入集合包含三個元素，Airflow 因此針對這三個輸入展開三個 Mapped Task Instances。

---

## Q3：`map_index` 可以區分什麼？

> 用來區分同一個 Mapped Task 中不同的 Mapping Instance。

例如：

```text
process_date[0]
process_date[1]
process_date[2]
```

其中：

```text
0
1
2
```

就是不同的 `map_index`。

---

## Q4：Dynamic Task Mapping 與 Python `for` loop 最大差別？

Python `for` loop：

```text
一個 Task Instance
↓
自己循環處理多筆資料
```

Dynamic Task Mapping：

```text
多筆輸入
↓
Airflow 展開多個 Mapped Task Instances
↓
分別管理 State / Log / Retry / Failure
```

---

## Q5：只有 `20260902` Failure，其他日期是否一定跟著 Failure？

不會。

例如：

```text
20260901 → Success
20260902 → Failed
20260903 → Success
```

因為三個日期對應不同的 Mapped Task Instances。

---

## Q6：100 個 Load Mapped Tasks 為什麼可能還需要 Pool？

因為：

> Dynamic Task Mapping 可以產生大量工作，但不代表外部系統能承受大量工作同時執行。

Pool 可以：

> 限制這些 Mapped Task Instances 同時間最多占用多少 Pool Slots，保護 PostgreSQL、API、Data Warehouse 等有限資源。

---

# 23. 常見錯誤

## 錯誤 1：把 Task Output 當普通 Python List

```python
trade_dates = get_trade_dates()

for trade_date in trade_dates:
    ...
```

❌

`get_trade_dates()` 在 DAG 建構時不是單純直接取得普通 Python List。

今天應該使用：

```python
trade_dates = get_trade_dates()

process_date.expand(
    trade_date=trade_dates
)
```

---

## 錯誤 2：使用 `for` loop 取代 Mapping

```python
for trade_date in trade_dates:
    process_date(trade_date)
```

❌

這沒有達成今天要學習的 Runtime Dynamic Task Mapping。

---

## 錯誤 3：把 Mapped Task Instances 稱為完全不同的 Task

例如：

```text
process_date[0]
process_date[1]
process_date[2]
```

它們是：

> 同一個 Mapped Task 的不同 Mapped Task Instances。

不是三個完全不同 `task_id` 的 Task。

---

## 錯誤 4：認為 Mapping 100 個就應該同時執行 100 個

❌

Dynamic Task Mapping：

```text
決定展開多少工作
```

不代表：

```text
所有工作必須同時執行
```

仍需要考慮：

- Worker Capacity
- Pool
- Concurrency
- Priority
- 外部系統容量

---

# 24. Day 31 核心整理

```text
expand()
→ 根據輸入集合動態展開 Mapped Task Instances

Mapped Task Instance
→ Airflow 可以分別管理每一份映射工作

map_index
→ 區分同一個 Mapped Task 裡不同的 Mapping Instance

Python for loop
→ 一個 Task Instance 裡循環處理多筆資料

Dynamic Task Mapping
→ Airflow 動態展開並管理多個 Mapped Task Instances

Pool
→ 控制大量 Mapped Tasks 使用有限資源時的併發容量
```

---

# 25. 今日記憶口訣

> **`expand()` 負責展開，`map_index` 負責區分，Airflow 負責分別管理。**

再把 Day 29～31 合起來：

> **Mapping 管工作數量，Pool 管併發容量，Priority 管排程優先。**

---

# Day 31 完成

**Airflow Dynamic Task Mapping（動態任務映射）✅**

目前進度：

```text
Day 28 ✅ Task Boundary / Architecture Review
Day 29 ✅ Pools & Concurrency
Day 30 ✅ Priority Weight / Scheduling Priority
Day 31 ✅ Dynamic Task Mapping

Next → Day 32
```