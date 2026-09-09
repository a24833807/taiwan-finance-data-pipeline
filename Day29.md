# Day 29｜Airflow Pools & Concurrency

## 今日主題

今天學習：

> **Airflow Pools & Concurrency：限制特定類型 Task 的同時執行數量，避免下游資源被大量併發工作壓垮。**

前面的課程已經學到：

```text
Day 27
Sensor poke / reschedule
→ 等待時如何使用 Worker Slot

Day 28
Task Boundary
→ 工作應該在哪裡切成 Airflow Task

Day 29
Pool / Concurrency
→ 多個 Task 都可以執行時，如何限制同時執行數量
```

---

# 1. 什麼是 Concurrency？

Concurrency 可以理解成：

> **同一時間允許多少工作並行執行。**

例如有：

```text
Task A
Task B
Task C
Task D
Task E
```

如果全部可以同時執行：

```text
A ─────────→
B ─────────→
C ─────────→
D ─────────→
E ─────────→

Time ──────→
```

代表 Concurrency 很高。

但：

> **可以同時執行，不代表應該全部同時執行。**

例如 20 個 Load Tasks 同時寫 PostgreSQL：

```text
Load 01 ─┐
Load 02 ─┤
Load 03 ─┤
...      ├──→ PostgreSQL
Load 19 ─┤
Load 20 ─┘
```

可能造成：

- 大量 DB Connections
- 大量 Queries
- 大量 Transactions
- Disk I/O 增加
- CPU / Memory 負載增加
- Database 效能下降

因此需要限制 Concurrency。

---

# 2. 什麼是 Airflow Pool？

Airflow Pool 是：

> **限制使用某個 Pool 的 Tasks 同一時間最多可以占用多少 Pool Slots 的機制。**

例如：

```text
postgres_pool
Slots = 2
```

有：

```text
Load A
Load B
Load C
Load D
```

全部指定：

```python
pool="postgres_pool"
```

如果每個 Task 需要 1 個 Pool Slot：

```text
postgres_pool

Slot 1 → Load A
Slot 2 → Load B

Load C → 等待
Load D → 等待
```

即使：

```text
Load A
Load B
Load C
Load D
```

在 Dependency 上全部都已經可以執行，Pool 仍然限制：

> 同時間最多占用 2 個 Pool Slots。

---

# 3. Pool 控制的不是整個 Airflow

這點非常重要。

假設：

```text
postgres_pool
Slots = 2
```

不是代表：

> 整個 Airflow 最多只能執行兩個 Tasks。

而是：

> **使用 `postgres_pool` 的 Tasks 受到這個 Pool 的限制。**

例如：

```text
Load A ─┐
Load B ─┼→ postgres_pool
Load C ─┘

Other Task
Another Task
```

`Other Task` 如果不受這個 Pool 限制，不能單純因為 `postgres_pool` 已滿，就認為它也一定不能執行。

因此：

```text
Pool
→ 特定類型 Task 的 Concurrency Control
```

---

# 4. Pool Slot 是什麼？

Pool 裡面設定：

```text
Slots = 1
```

代表這個 Pool 可提供的容量是 1 個 Pool Slot。

例如：

```text
learning_pool
Slots = 1
```

三個 Tasks：

```text
task_a
task_b
task_c
```

全部：

```python
pool="learning_pool"
```

如果每個 Task 都占 1 個 Pool Slot：

```text
learning_pool

Slot 1 → task_a Running

task_b → 等待
task_c → 等待
```

當 `task_a` 完成：

```text
task_a
↓
Success
↓
釋放 Pool Slot
↓
task_b / task_c 其中一個取得 Slot
```

---

# 5. Pool Slots = 2

如果：

```text
learning_pool
Slots = 2
```

則：

```text
Slot 1 → task_a
Slot 2 → task_b

task_c → 等待
```

因此同時間最多可以有兩個各占 1 Slot 的 Tasks 使用這個 Pool。

注意：

> 哪兩個 Task 先取得 Slot，不應假設一定固定是 A、B。

Scheduler 會根據當時實際狀態安排。

---

# 6. Pool Slot ≠ Worker Slot

這是 Day 29 最重要的區別之一。

## Worker Capacity

回答的是：

> **Airflow 執行環境整體還有沒有能力執行 Task？**

可以簡化理解成 Airflow 的整體執行資源。

---

## Pool

回答的是：

> **某一類 Task 被允許同時使用多少容量？**

例如：

```text
Worker Capacity
= 10

postgres_pool
= 2 Slots
```

目前：

```text
Load A → postgres_pool Slot 1
Load B → postgres_pool Slot 2
```

雖然 Worker 可能還有很多執行 Capacity：

```text
Worker
██████████░░░░░░░░░░
還有空間
```

但是：

```text
postgres_pool
██
已滿
```

所以：

```text
Load C
↓
不能因為 Worker 有空就直接執行
↓
必須等待 postgres_pool Slot
```

---

# 7. 最重要的一句

> **Worker 有資源 ≠ Pool 有額度。**

Task 真正能不能執行，不能只看 Worker Capacity。

還可能受到：

```text
Pool
Concurrency Limit
Dependency
Scheduler
其他 Airflow 限制
```

影響。

Day 29 目前先聚焦：

```text
Worker Capacity
vs
Pool Capacity
```

---

# 8. 為什麼需要 Pool？

Pool 的目的不只是：

> 不要讓 Airflow 跑太多 Task。

更重要的是：

> **保護有限的 External Resource。**

例如：

```text
Airflow
   ↓
PostgreSQL
```

假設 Airflow 有能力同時執行 20 個 Tasks：

```text
20 Load Tasks
↓
Worker 有足夠 Capacity
↓
全部同時寫 PostgreSQL
```

Airflow 自己可能沒有問題。

但是：

```text
PostgreSQL
```

不一定希望瞬間承受這麼高的併發。

因此建立：

```text
postgres_pool
Slots = 3
```

變成：

```text
20 Load Tasks
       ↓
┌──────────────────┐
│  postgres_pool   │
│                  │
│ Slot 1           │
│ Slot 2           │
│ Slot 3           │
└────────┬─────────┘
         ↓
     PostgreSQL
```

同時間最多讓 3 個各占 1 Slot 的 Load Tasks 進入。

其他：

```text
Load 04
Load 05
...
Load 20
```

等待 Pool Capacity。

---

# 9. Pool 可以保護哪些東西？

除了 Database：

```text
PostgreSQL
MySQL
Data Warehouse
```

還可能是：

```text
External API
第三方 Service
有限資源的 Compute System
其他具有 Concurrency / Rate 限制的系統
```

例如 API：

```text
100 Tasks
↓
external_api_pool
Slots = 5
↓
External API
```

避免大量 Task 同時打 API。

但要注意：

> Pool 是 Concurrency Control，不等於完整的 API Rate Limiter。

例如：

```text
API 限制
100 requests / minute
```

不能單靠「Pool = 5」就精確保證每分鐘只有 100 Requests。

這是不同問題。

---

# 10. Day 29 Demo

今天建立：

```text
airflow/dags/pool_demo_dag.py
```

三個 Tasks：

```text
        task_a
       ↗
start ─→ task_b
       ↘
        task_c
```

重要的是：

```text
task_a
task_b
task_c
```

Dependency 本身允許平行。

不是：

```text
task_a
↓
task_b
↓
task_c
```

否則它們本來就只能依序執行，無法證明 Pool 的效果。

---

# 11. Demo Task

概念：

```python
@task(pool="learning_pool")
def task_a():
    print("Task A started")
    time.sleep(10)
    print("Task A finished")
```

另外：

```text
task_b
task_c
```

也使用：

```python
pool="learning_pool"
```

因此：

```text
三個 Tasks
↓
共享 learning_pool 的 Capacity
```

---

# 12. 實驗一：Slots = 1

設定：

```text
learning_pool
Slots = 1
```

Dependency：

```text
task_a ─→
task_b ─→    理論上都可以 Parallel
task_c ─→
```

但是 Pool：

```text
learning_pool
Slots = 1
```

所以：

```text
某一個 Task → Running
另外兩個    → 等待
```

完成一個：

```text
Task Success
↓
釋放 Slot
↓
下一個 Task 取得 Slot
```

因此：

> 三個 Task 雖然 Dependency 允許 Parallel，但 Pool 強制它們同時間最多占用 1 個 Slot。

---

# 13. 實驗二：Slots = 2

修改：

```text
learning_pool
Slots = 2
```

重新 Trigger。

現在：

```text
Task A → Running
Task B → Running
Task C → 等待
```

最多兩個各占 1 Slot 的 Task 同時執行。

其中一個完成：

```text
Slot Free
↓
Task C
↓
Running
```

因此可以直接觀察：

```text
Pool Slots
↓
影響允許的 Concurrency
```

---

# 14. Dependency vs Pool

這兩個概念不要混在一起。

## Dependency

決定：

> Task 在流程上是否已經具備執行資格。

例如：

```text
Extract
↓
Transform
```

Transform 必須等 Extract。

---

## Pool

決定：

> 即使 Task 已經具備執行資格，Pool 是否還有 Capacity 讓它執行。

例如：

```text
Task Ready
↓
Dependency OK
↓
Pool 有 Slot？
├─ Yes → 可以繼續進入執行流程
└─ No  → 等待
```

所以：

```text
Dependency
→ 流程限制

Pool
→ 資源 / Concurrency 限制
```

---

# 15. Pool 等待 ≠ Failure

假設：

```text
postgres_pool
Slots = 2
```

現在：

```text
Load A → Running
Load B → Running
Load C → 等 Pool
```

`Load C` 沒有執行失敗。

只是：

> Pool Capacity 暫時不足。

因此概念上：

```text
Pool Full
≠
Task Failure
```

這跟之前 Sensor 的觀念有點類似：

```text
Sensor False
≠ Failure

Reschedule
≠ Failure

Pool 沒 Slot
≠ Failure
```

它們背後原因不同，但都不能把「現在不能繼續」直接等同於「執行失敗」。

---

# 16. Day 27 與 Day 29 的差別

Day 27：

```text
Sensor
↓
等待 External Condition
↓
等待期間要不要占 Worker Slot？
```

學：

```text
poke
vs
reschedule
```

Day 29：

```text
很多 Task 都 Ready
↓
到底允許多少一起執行？
```

學：

```text
Pool
Concurrency
```

因此：

```text
Day 27
→ Waiting Resource Behavior

Day 29
→ Concurrent Execution Control
```

---

# 17. Pool vs Retry

也不要混淆。

## Pool

```text
Task Ready
↓
Pool Full
↓
等待 Capacity
```

不是 Failure。

---

## Retry

```text
Task 已經執行
↓
Failure
↓
還有 Retry
↓
稍後重新 Attempt
```

所以：

```text
Pool
→ 控制誰現在可以進去執行

Retry
→ 執行失敗之後重新嘗試
```

---

# 18. 套用到 TWSE Pipeline

目前正式 TWSE Pipeline 還不需要為了展示功能硬加 Pool。

但未來如果架構變成：

```text
TWSE Stock Load ──────┐
TWSE Index Load ──────┤
Backfill Load ────────┼→ PostgreSQL
Other Pipeline ───────┤
Historical Load ──────┘
```

就可以考慮：

```text
postgres_pool
```

例如：

```text
postgres_pool
Slots = 3
```

限制：

> 同時間最多讓一定數量的 Load Tasks 使用 PostgreSQL。

這跟 Day 28 的原則一致：

> **學會 Airflow Feature，不代表正式 Pipeline 現在一定要使用。**

有真正 Requirement 再加入。

---

# 19. Day 29 五題正式答案

## Q1：Pool 主要控制什麼？

> Pool 用來限制使用該 Pool 的 Tasks，同一時間最多可以占用多少 Pool Slots，藉此控制這類工作的 Concurrency。

不是限制整個 Airflow 的 Task 數量。

---

## Q2：learning_pool 有 1 Slot，三個 Task 都使用它，最多幾個同時執行？

如果每個 Task 占 1 Slot：

```text
最多 1 個
```

其他 Task 等待 Pool Capacity。

---

## Q3：改成 2 Slots 呢？

如果每個 Task 占 1 Slot：

```text
最多 2 個
```

第三個等待其中一個 Slot 被釋放。

---

## Q4：Worker 還有很多空閒 Capacity，Task 一定能執行嗎？

> 不一定。

因為：

```text
Worker Capacity
```

和：

```text
Pool Capacity
```

是不同的限制。

即使 Worker 還有 Capacity：

```text
postgres_pool
```

如果已經沒有可用 Slot，使用這個 Pool 的 Task 仍然需要等待。

記住：

> **Worker 有資源 ≠ Pool 有額度。**

---

## Q5：為什麼 20 個 Load Tasks 可能需要 postgres_pool？

因為如果 20 個 Load Tasks 同時寫 PostgreSQL，可能產生：

```text
大量 Connections
大量 Queries
大量 Transactions
大量 I/O
```

造成 Database 負載過高。

因此可以：

```text
20 Load Tasks
       ↓
postgres_pool
Slots = 3
       ↓
PostgreSQL
```

限制同時寫入 PostgreSQL 的工作數量，以保護下游 Database。

---

# 20. Day 29 核心架構

```text
                    Airflow
                       │
              Worker Capacity
                       │
              ┌────────┴────────┐
              │                 │
          Other Tasks      Load Tasks
                                │
                                ↓
                         postgres_pool
                           Slots = 3
                                │
                       ┌────────┼────────┐
                       ↓        ↓        ↓
                     Load     Load     Load
                       │        │        │
                       └────────┼────────┘
                                ↓
                           PostgreSQL
```

Pool 是在：

```text
Airflow Tasks
↓
External Resource
```

之間增加一層 Concurrency Control。

---

# 21. Day 29 最重要的四句

```text
Concurrency
→ 同一時間有多少工作並行

Pool
→ 限制特定 Tasks 可以使用的並行容量

Worker Capacity
→ Airflow 整體執行能力

Pool Capacity
→ 某類 Task 被允許使用的容量
```

以及最重要的一句：

> **Worker 有資源 ≠ Pool 有額度。**

---

# 22. Day 29 與目前學習路線

目前已經從：

```text
Airflow 基礎
↓
Schedule / Logical Date
↓
Catchup / Backfill
↓
Dependency
↓
XCom
↓
Branching
↓
Variables / Connections
↓
TaskGroup
↓
Retry / Timeout
↓
Failure Callback
↓
Sensor
↓
poke / reschedule
↓
Task Boundary / Architecture Review
↓
Pools & Concurrency
```

逐漸從：

```text
「DAG 怎麼寫」
```

進入：

```text
「Airflow 如何管理 Production Data Pipeline」
```

---

# Day 29 完成

**Airflow Pools & Concurrency ✅**

今日核心：

> **Pool 的價值不是單純讓 Task 跑慢一點，而是對特定工作建立 Concurrency Boundary，避免 Airflow 的執行能力超過下游系統能安全承受的能力。**
