# Day 30｜Airflow Priority Weight & Scheduling Priority

## 1. 今日主題

今天學習：

> **Airflow Priority Weight（優先權重）**：當多個 Task 同時競爭有限的排程資源時，讓 Scheduler（排程器）知道哪些 Task 具有較高的 Scheduling Priority（排程優先級）。

Day 29 與 Day 30 可以串在一起理解：

```text
Day 29｜Pool & Concurrency
→ 同時最多允許多少工作占用有限資源？

Day 30｜Priority Weight
→ 多個工作競爭有限資源時，誰的排程優先級比較高？
```

---

# 2. 為什麼需要 Priority？

假設：

```text
learning_pool
Slots = 1
```

現在有三個 Task：

```text
high_priority
medium_priority
low_priority
```

三個 Task：

- 都已經具備執行條件
- 彼此沒有 Dependency（相依關係）
- 都使用 `learning_pool`
- 都想取得有限的 Pool Slot

但是：

```text
learning_pool
Slots = 1
```

同時間可使用的 Pool Capacity（資源池容量）有限。

因此產生問題：

> 多個 Task 都想取得有限資源時，Scheduler 應該優先考慮誰？

這就是 Priority Weight 要處理的問題之一。

---

# 3. priority_weight 是什麼？

Task 可以設定：

```python
priority_weight=10
```

代表：

> 這個 Task 具有較高的排程優先權重。

例如：

```text
high_priority
priority_weight = 10

medium_priority
priority_weight = 5

low_priority
priority_weight = 1
```

在適用的資源競爭情境下：

```text
10 > 5 > 1
```

因此：

```text
high_priority
→ Scheduling Priority 較高

medium_priority
→ Scheduling Priority 次之

low_priority
→ Scheduling Priority 較低
```

---

# 4. priority_weight 是 Task 的設定

這點很重要。

不是：

```text
Pool
priority_weight = 10
```

而是：

```text
Task
priority_weight = 10
```

例如：

```python
@task(
    pool="learning_pool",
    priority_weight=10,
)
def high_priority():
    ...
```

所以：

```text
Pool
→ 管理有限的 Concurrency Capacity（併發容量）

Priority Weight
→ 表達 Task 的排程優先權重
```

---

# 5. Priority Weight 越大代表什麼？

例如：

```text
Task A
priority_weight = 10

Task B
priority_weight = 1
```

當兩者同時競爭有限資源時：

```text
Task A
→ 較高 Scheduling Priority

Task B
→ 較低 Scheduling Priority
```

因此：

> 數值較大的 Task 具有較高的 Priority Weight。

但是必須注意：

> **Priority 高，不代表保證固定的 Execution Order（執行順序）。**

---

# 6. Priority ≠ Execution Order

這是 Day 30 最重要的觀念。

假設：

```text
A
priority_weight = 10

B
priority_weight = 1
```

這表示：

> A 在適用的資源競爭情境下具有較高的 Scheduling Priority。

但不能理解成：

```text
A 一定先執行
↓
A 一定先完成
↓
B 才能執行
```

也就是：

```text
Priority
≠
固定執行順序
```

正確理解：

```text
Priority
=
Scheduling Priority / Scheduling Preference
排程優先級 / 排程偏好
```

實際排程還可能受到：

- Task 何時進入可排程狀態
- Pool Capacity
- 其他 Concurrency Limit
- Executor
- Scheduler 當時狀態
- 其他排程條件

等因素影響。

因此：

> **Priority Weight 不應該拿來表達 Business Dependency（業務相依關係）。**

---

# 7. 如果 A 一定完成後才能執行 B？

如果 Requirement（需求）是：

```text
A
↓
完成
↓
B 才能執行
```

應該使用：

> **Dependency（相依關係）**

例如：

```python
a >> b
```

代表：

```text
A
↓
B
```

所以：

```text
Priority
→ 誰比較優先

Dependency
→ 誰必須等誰
```

---

# 8. Pool、Priority、Dependency 的差別

這三個概念必須分開。

## Pool

回答：

> 同時間最多允許多少資源被占用？

例如：

```text
learning_pool
Slots = 2
```

---

## Priority

回答：

> 多個 Task 競爭有限資源時，誰的排程優先級比較高？

例如：

```text
Daily Pipeline
priority_weight = 10

Historical Backfill
priority_weight = 1
```

---

## Dependency

回答：

> 誰必須等誰？

例如：

```text
Extract
↓
Transform
↓
Load
```

---

## 三者記憶方式

```text
Pool
→ 一次能進多少

Priority
→ 競爭時誰比較優先

Dependency
→ 誰必須等誰
```

---

# 9. Day 30 Demo

新增：

```text
airflow/dags/priority_demo_dag.py
```

DAG：

```text
priority_demo
```

包含三個 Task：

```text
high_priority
medium_priority
low_priority
```

三個 Task 彼此沒有 Dependency，因此理論上可以平行：

```text
high_priority   ───→

medium_priority ───→

low_priority    ───→
```

三個 Task 全部使用：

```text
pool="learning_pool"
```

並設定：

```text
high_priority
priority_weight = 10

medium_priority
priority_weight = 5

low_priority
priority_weight = 1
```

Pool：

```text
learning_pool
Slots = 1
```

形成：

```text
high    weight=10 ─┐
medium  weight=5  ─┼──→ learning_pool
low     weight=1  ─┘

                        Slots = 1
```

目的：

> 建立有限資源競爭情境，觀察 Priority Weight 對 Scheduling Priority 的影響。

---

# 10. 實際遇到的問題：Pool 不存在

第一次執行時 Airflow 顯示：

```text
Tasks using non-existent pool 'learning_pool'
will not be scheduled
```

Task 停留在：

```text
scheduled
```

原因：

> DAG 裡雖然指定了 `pool="learning_pool"`，但 Airflow 當時找不到 `learning_pool`。

這代表：

```python
pool="learning_pool"
```

只是告訴 Airflow：

> 這個 Task 要使用 `learning_pool`。

並不代表：

> Airflow 會自動建立 `learning_pool`。

---

# 11. Pool 必須先存在

Airflow 中需要建立：

```text
Pool Name:
learning_pool

Slots:
1
```

DAG Task 再指定：

```python
pool="learning_pool"
```

概念流程：

```text
Task
pool="learning_pool"
        ↓
Airflow 尋找 learning_pool
        ↓
Pool 存在？
├─ Yes
│   ↓
│ 進入後續排程判斷
│
└─ No
    ↓
Task 無法使用該 Pool 被正常排程
```

因此：

> **在 DAG 裡引用 Pool，不等於建立 Pool。**

---

# 12. Pool 與 Priority 如何合作？

概念：

```text
             Ready Tasks
                  │
       ┌──────────┼──────────┐
       ↓          ↓          ↓
     High       Medium       Low
      10           5          1
       │           │          │
       └───────────┼──────────┘
                   ↓
             learning_pool
                Slots=1
                   ↓
             有限執行容量
```

Pool 負責：

```text
有限 Capacity
```

Priority 負責：

```text
Task 之間的排程優先級
```

因此：

> **Pool 解決 Capacity 問題；Priority 解決有限資源競爭時的優先級問題。**

---

# 13. Production 使用情境

假設未來同時存在：

```text
Daily Pipeline
Historical Backfill
```

Daily Pipeline：

```text
今天正式資料
↓
通常具有較高時效性
```

Historical Backfill：

```text
補歷史資料
↓
通常可以稍晚完成
```

如果大量 Backfill 同時競爭有限資源：

```text
Backfill 01 ─┐
Backfill 02 ─┤
Backfill 03 ─┤
...          ├──→ 有限資源
Backfill 20 ─┘

Daily Pipeline ─────→ 也需要資源
```

可能設定：

```text
Daily Pipeline
priority_weight = 10

Historical Backfill
priority_weight = 1
```

目的：

> 降低大量低時效性的 Historical Backfill 延遲 Daily Pipeline 的風險。

---

# 14. 為什麼 Daily Pipeline 可以設定較高 Priority？

不是因為：

```text
Daily Pipeline
天生一定要 priority_weight=10
```

而是根據 Business Requirement（業務需求）。

例如：

```text
Daily Pipeline
↓
每天早上必須完成
↓
下游報表依賴
↓
時效性高
```

而：

```text
Historical Backfill
↓
補過去資料
↓
晚幾個小時影響較低
```

因此：

```text
Daily
→ Higher Priority

Backfill
→ Lower Priority
```

才具有架構上的理由。

所以：

> **Priority 應該反映工作的重要性與時效需求，而不是隨便設定較大的數字。**

---

# 15. Priority 不能取代 Dependency

錯誤設計：

```text
Extract
priority_weight = 30

Transform
priority_weight = 20

Load
priority_weight = 10
```

然後期待：

```text
Extract
↓
Transform
↓
Load
```

這是不正確的。

如果 ETL Requirement 是：

```text
Extract 完成
↓
Transform 才能執行
↓
Transform 完成
↓
Load 才能執行
```

應該建立：

```python
extract >> transform >> load
```

Priority 不負責描述：

- Data Flow（資料流程）
- Business Flow（業務流程）
- Upstream / Downstream（上下游相依）

Dependency 才負責。

---

# 16. 與 Task Boundary 的關係

Day 28 學過：

> **Task Boundary（Task 邊界）= 工作在哪裡切成獨立 Airflow Task。**

如果未來：

```text
Daily Load Task
Historical Backfill Load Task
```

是不同 Task：

```text
Daily Load
priority = high

Backfill Load
priority = low
```

Airflow 才能對不同 Task 做不同的排程控制。

所以：

```text
Task Boundary
↓
哪些工作成為獨立 Task

Dependency
↓
Task 之間誰依賴誰

Pool
↓
Task 同時最多能使用多少有限資源

Priority
↓
競爭有限資源時誰比較優先
```

---

# 17. Day 30 五題正式答案

## Q1：`priority_weight=10` 是什麼？

表示：

> 該 Task 擁有較高的排程優先權重。在適用的有限資源競爭情境下，Scheduler 可以利用 Priority Weight 判斷 Scheduling Priority。

它是：

```text
Task Setting
```

不是：

```text
Pool Setting
```

---

## Q2：`priority_weight=10` 和 `priority_weight=1` 哪個較高？

```text
priority_weight = 10
```

較高。

也就是：

```text
10
→ Higher Scheduling Priority

1
→ Lower Scheduling Priority
```

---

## Q3：Priority Weight 是否保證 Task 一定先執行？

**不保證。**

Priority 是：

```text
Scheduling Priority / Scheduling Preference
```

不是：

```text
Guaranteed Execution Order
```

如果需要：

```text
A 完成
↓
B 才執行
```

應使用：

```text
Dependency
```

---

## Q4：A 一定完成才能 B，應該使用什麼？

使用：

> **Dependency**

例如：

```python
a >> b
```

而不是：

```text
A priority = 10
B priority = 1
```

---

## Q5：為什麼 Daily Pipeline 可以比 Historical Backfill Priority 高？

因為：

> Daily Pipeline 通常具有較高時效性，而 Historical Backfill 通常可以稍晚完成。

因此有限資源競爭時：

```text
Daily Pipeline
priority = high

Historical Backfill
priority = low
```

可以降低大量 Backfill 延遲正式 Daily Processing 的風險。

---

# 18. Day 29 + Day 30 整合

假設：

```text
Worker Capacity = 足夠

postgres_pool
Slots = 2
```

現在有：

```text
Daily A       Priority 10
Daily B       Priority 10

Backfill A    Priority 1
Backfill B    Priority 1
Backfill C    Priority 1
```

架構：

```text
                    Ready Tasks
                        │
        ┌───────────────┼───────────────┐
        ↓               ↓               ↓
      Daily          Backfill        Backfill
   High Priority    Low Priority    Low Priority
        │               │               │
        └───────────────┼───────────────┘
                        ↓
                  postgres_pool
                     Slots=2
                        ↓
                   PostgreSQL
```

因此：

```text
Pool
→ 保護 PostgreSQL
→ 限制同時使用資源的 Task 數量

Priority
→ 資源競爭時提高重要工作的排程優先級
```

---

# 19. Day 27～30 串聯

```text
Day 27
Sensor poke vs reschedule
→ 等待時是否占 Worker Slot

Day 28
Task Boundary
→ 工作應該在哪裡切成 Task

Day 29
Pool & Concurrency
→ 同時允許多少工作使用有限資源

Day 30
Priority Weight
→ 多個工作競爭有限資源時誰比較優先
```

概念化：

```text
Airflow Scheduler
       ↓
Dependency
誰已經具備執行條件？
       ↓
Ready Tasks
       ↓
Pool
有多少有限容量？
       ↓
Priority
競爭時誰比較優先？
       ↓
Executor / Worker
```

這是用來理解不同機制責任的概念模型，不需要當成 Airflow 內部完整實作流程來背。

---

# 20. 常見錯誤

## 錯誤 1

```text
priority_weight = 10
→ 保證第一個執行
```

❌

正確：

```text
priority_weight = 10
→ 較高 Scheduling Priority
```

---

## 錯誤 2

```text
Pool Slots = 1
→ 決定 A → B → C 的執行順序
```

❌

正確：

```text
Pool Slots = 1
→ 限制同時占用 Pool Capacity 的數量
```

---

## 錯誤 3

```text
Priority
→ 控制 Dependency
```

❌

正確：

```text
Dependency
→ 誰必須等誰

Priority
→ 競爭有限資源時誰比較優先
```

---

## 錯誤 4

```text
pool="learning_pool"
→ Airflow 自動建立 Pool
```

❌

正確：

```text
pool="learning_pool"
→ Task 指定使用 learning_pool

learning_pool
→ 必須實際存在於 Airflow
```

---

# 21. Day 30 核心整理

```text
Pool
→ 控制「同時最多能進多少」

Priority
→ 控制「競爭時誰的排程優先級較高」

Dependency
→ 控制「誰必須等誰」

priority_weight
→ Task 的排程權重，不是 Pool 的設定
```

最重要：

> **Priority 是排程偏好，不是執行順序保證；需要保證順序時使用 Dependency。**

---

# 22. 今日記憶口訣

> **Pool 管數量，Priority 管優先，Dependency 管先後。**

---

# Day 30 完成

**Airflow Priority Weight & Scheduling Priority ✅**

目前進度：

```text
Day 28 ✅ Task Boundary / Architecture Review
Day 29 ✅ Pools & Concurrency
Day 30 ✅ Priority Weight / Scheduling Priority

Next → Day 31
```