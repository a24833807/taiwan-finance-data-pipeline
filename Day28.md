# Day 28｜Airflow Formal DAG Architecture Review

## 今日主題

前面已經學過許多 Airflow 核心功能：

```text
Schedule / Logical Date
Task Dependency
Trigger Rule
XCom
Branching
Variable / Connection
TaskGroup
Retry / Timeout
Failure Callback
Sensor
poke / reschedule
```

Day 28 不再增加新的 Operator，而是回頭檢查正式的：

```text
TWSE Finance Data Pipeline
```

核心問題：

> **學過這麼多 Airflow 功能之後，哪些真的應該放進正式 Pipeline？**

以及：

> **目前為什麼仍然使用一個 Airflow Task 包住整個 Python ETL？**

今天開始從「會使用 Airflow 功能」進入：

```text
Data Pipeline Architecture Design
```

---

# 1. 目前 TWSE Pipeline 架構

目前架構：

```text
Airflow DAG
    ↓
run_twse_pipeline
    ↓
run_pipeline(trade_date)
    ↓
Extract
    ↓
Transform
    ↓
Validate
    ↓
Load
    ↓
PostgreSQL
```

在 Airflow 看來：

```text
┌──────── Airflow Task ────────┐
│                              │
│      run_twse_pipeline       │
│              ↓               │
│        run_pipeline()        │
│              ↓               │
│          Extract             │
│              ↓               │
│         Transform            │
│              ↓               │
│         Validate             │
│              ↓               │
│           Load               │
│                              │
└──────────────────────────────┘
```

也就是：

> Extract、Transform、Validate、Load 雖然是不同 Python ETL Stage，但目前仍屬於同一個 Airflow Task。

---

# 2. 為什麼目前 TWSE Pipeline 維持一個 Airflow Task？

這題是 Day 28 最重要的問題之一。

不能只回答：

> 因為目前 Pipeline 很簡單。

這個方向沒有錯，但還不夠完整。

真正的核心原因是：

> **目前 ETL 各 Stage 之間直接透過同一個 Python Process 的記憶體傳遞 Pandas DataFrame，還沒有 External Storage 作為跨 Task 的資料交換層。**

目前：

```text
同一個 Airflow Task
        ↓
同一段 Python Pipeline
        ↓

Extract
   ↓
 DataFrame
   ↓
Transform
   ↓
 DataFrame
   ↓
Validate
   ↓
 DataFrame
   ↓
 Load
```

這種架構下：

```python
raw_df = extract(...)

transformed_df = transform(raw_df)

valid_df = validate(transformed_df)

load(valid_df)
```

DataFrame 可以直接在 Python Function 之間傳遞。

不需要：

```text
External Storage
XCom
Serialization
跨 Task 資料交換
```

---

# 3. 如果現在硬拆成四個 Airflow Tasks？

假設改成：

```text
Extract Task
    ↓
Transform Task
    ↓
Validate Task
    ↓
Load Task
```

最大的問題不是：

```text
Airflow 不支援
```

Airflow 當然支援。

真正的問題是：

> **Task 之間不能假設共享上一個 Task 記憶體中的 DataFrame。**

也就是：

```text
Extract Task
↓
產生 DataFrame
↓
Task 結束

Transform Task
↓
？？？
```

Transform 必須知道：

> Extract 的資料在哪裡？

因此一旦切開 Task Boundary，就必須同時設計：

```text
Cross-Task Data Exchange
```

---

# 4. 為什麼不能直接把 DataFrame 丟進 XCom？

很容易想到：

```text
Extract
↓
DataFrame
↓
XCom
↓
Transform
```

但這不是好的設計。

Day 19 已經學過：

> **XCom 適合小型 Metadata，不適合大型 Dataset。**

XCom 比較適合：

```text
trade_date

row_count

status

table_name

file_path

S3 URI

S3 Object Key
```

例如：

```text
s3://finance-data/raw/20260908.parquet
```

而不是：

```text
幾十萬筆股票資料
大型 Pandas DataFrame
大型 JSON
大型 Binary Data
```

因此：

```text
XCom
→ 傳資料的位置

External Storage
→ 存真正的資料
```

這是非常重要的架構分工。

---

# 5. 所以目前維持一個 Task 的完整理由

目前：

```text
TWSE API
↓
Extract
↓
DataFrame
↓
Transform
↓
DataFrame
↓
Validate
↓
DataFrame
↓
Load
↓
PostgreSQL
```

資料量與流程目前仍可以合理地在：

```text
單一 Python Pipeline
```

內完成。

而且目前還沒有：

```text
S3
Object Storage
Raw Data Layer
Intermediate Data Layer
```

因此現在如果只是為了讓 DAG 看起來漂亮而拆成：

```text
Extract Task
↓
Transform Task
↓
Validate Task
↓
Load Task
```

反而會額外產生：

```text
跨 Task DataFrame 傳遞問題
```

甚至可能導致錯誤設計：

```text
Large DataFrame
↓
XCom
```

因此目前：

```text
One Airflow Task
↓
run_pipeline()
```

仍然是一個合理的架構選擇。

### 第一題正式答案

> **目前 TWSE Pipeline 的 ETL 流程相對單純，而且 Extract、Transform、Validate、Load 之間目前直接透過同一個 Python Process 傳遞 DataFrame。若現在拆成多個 Airflow Tasks，就必須額外處理跨 Task 的資料交換，而大型 DataFrame 又不適合透過 XCom 傳遞。目前尚未加入 S3 / Object Storage 等 External Storage，因此維持單一 Airflow Task 包裝 `run_pipeline()`，可以避免不必要的架構複雜度。**

這個答案比：

```text
因為目前應用單純
```

完整很多。

---

# 6. Airflow Task Boundary 是什麼？

Task Boundary 可以理解成：

> **決定一段工作在哪裡切成獨立 Airflow Task 的責任與執行邊界。**

目前：

```text
┌────────────── Task Boundary ──────────────┐
│                                          │
│              Extract                     │
│                 ↓                        │
│             Transform                    │
│                 ↓                        │
│             Validate                     │
│                 ↓                        │
│               Load                       │
│                                          │
└──────────────────────────────────────────┘
```

只有一個 Airflow Task Boundary。

未來可能：

```text
┌── Task Boundary ──┐
│      Extract      │
└─────────┬─────────┘
          ↓
     S3 Raw Layer
          ↓
┌── Task Boundary ──┐
│     Transform     │
└─────────┬─────────┘
          ↓
  S3 Processed Layer
          ↓
┌── Task Boundary ──┐
│     Validate      │
└─────────┬─────────┘
          ↓
┌── Task Boundary ──┐
│       Load        │
└───────────────────┘
```

這時就有多個 Task Boundaries。

---

# 7. 為什麼 Task Boundary 很重要？

因為 Airflow 是以：

```text
Task
```

作為重要的執行管理單位。

假設：

```text
Extract     Success
Transform   Failed
Validate    upstream_failed
Load        upstream_failed
```

如果每個 Stage 是獨立 Task，Airflow 可以清楚看到每個 Stage 的狀態。

也可以個別設定：

```text
Retry
Timeout
Dependency
Trigger Rule
Monitoring
Failure Handling
```

例如：

```text
Extract
retries=3

Transform
retries=1

Load
execution_timeout=10 min
```

因此 Task Boundary 不只是：

```text
程式切成幾個 Function
```

而是：

> **Airflow 如何管理這段工作的執行、狀態、Retry、Timeout、Dependency 與 Monitoring。**

---

# 8. Task 是不是拆越細越好？

不是。

錯誤觀念：

```text
1 個 Task
→ 不專業

4 個 Tasks
→ 比較專業

10 個 Tasks
→ 更專業
```

這是不成立的。

每切一個 Task Boundary，都需要考慮：

```text
資料怎麼傳？
失敗怎麼處理？
Retry 是否安全？
中間結果放哪？
是否值得獨立 Monitoring？
是否真的需要獨立執行？
```

因此：

> **Task Boundary 是 Architecture Decision，不是越細越好。**

---

# 9. 什麼情況值得拆成不同 Task？

可以思考：

```text
這個 Stage 是否需要獨立 Retry？
              ↓
這個 Stage 是否需要獨立 Monitoring？
              ↓
是否需要獨立 Timeout？
              ↓
是否可能獨立執行？
              ↓
是否有合理的跨 Task Data Exchange？
```

如果答案逐漸變成 Yes：

```text
建立獨立 Task Boundary
```

就開始有價值。

---

# 10. 拆 Task 的優點

假設：

```text
Extract
↓
Transform
↓
Validate
↓
Load
```

都拆成 Airflow Task。

如果 Transform Failure：

```text
Extract       Success
   ↓
Transform     Failed
   ↓
Validate      upstream_failed
   ↓
Load          upstream_failed
```

Airflow UI 可以直接看到：

```text
Transform Failed
```

而不是只有：

```text
run_twse_pipeline Failed
```

因此優點包括：

```text
更清楚的 Task State
更細粒度的 Retry
更細粒度的 Timeout
更好的 Monitoring
更容易定位 Failure
```

---

# 11. 拆 Task 的代價

另一方面：

```text
Extract Task
↓
Transform Task
```

代表：

> Extract 的 Output 必須有辦法讓 Transform 取得。

不能單純依賴：

```text
Python Memory
```

因此需要：

```text
External Storage
Database
Object Storage
File Storage
```

或其他合理的 Data Exchange Mechanism。

所以：

```text
更多 Task
≠
免費得到更好的架構
```

而是：

```text
更多 Task
↓
更細的 Orchestration
+
更複雜的 Data Exchange
```

---

# 12. 未來加入 S3 / Object Storage

未來加入 AWS 後，可以改成：

```text
TWSE API
   ↓
Extract Task
   ↓
Raw Data
   ↓
S3 Raw Layer
   │
   │ XCom
   │ "s3://.../raw/20260908.parquet"
   ↓
Transform Task
   ↓
讀取 S3 Raw
   ↓
Transform
   ↓
寫入 S3 Processed
   ↓
Validate / Load
```

這時：

```text
真正資料
→ S3

資料位置
→ XCom
```

責任非常清楚。

---

# 13. External Storage + XCom 分工

最重要的 Pattern：

```text
             Large Data
Extract ───────────────────→ S3
   │
   │ Small Metadata
   ↓
 XCom
   │
   │ S3 URI / Object Key
   ↓
Transform ─────────────────→ Read S3
```

因此：

```text
External Storage
→ Data Plane
→ 存大量資料

XCom
→ Control / Metadata
→ 告訴下一個 Task 資料在哪裡
```

在目前學習階段，可以先用這種方式理解兩者的責任。

---

# 14. 正式 DAG 功能 Review

## Schedule / Logical Date

需要：

```text
Daily Schedule
↓
Logical Date / Data Interval
↓
決定 trade_date
```

不應使用：

```python
datetime.now()
```

決定歷史資料日期。

因為還需要支援：

```text
Retry
Backfill
Historical Run
```

---

# 15. Retry / Retry Delay

適合正式 Pipeline。

例如：

```text
TWSE API 暫時 Timeout
↓
Failure
↓
Retry Delay
↓
Retry
```

適合：

```text
Network Error
API Temporary Error
DB Connection Error
Temporary Timeout
```

不適合：

```text
Syntax Error
Import Error
錯誤參數
固定會失敗的 Business Logic
```

---

# 16. execution_timeout

適合正式 Pipeline。

防止：

```text
Task
↓
Hang
↓
永遠 Running
```

可以設定：

```text
execution_timeout
```

限制：

> 單次 Task Attempt 最大允許執行時間。

---

# 17. on_failure_callback

正式 Pipeline 也適合。

流程：

```text
Task Failure
↓
Retry
↓
Retry
↓
Final Failed
↓
on_failure_callback
↓
Logging / Alert
```

未來可以接：

```text
Slack
Teams
Email
PagerDuty
```

目前作品集不需要急著加入所有通知系統。

---

# 18. Sensor 現在需要嗎？

目前：

```text
Airflow
↓
TWSE API
```

沒有明確：

```text
等待上游 File
等待其他 DAG
等待 Database Ready
```

所以：

```text
Sensor
→ 暫時不需要
```

重點：

> 學會 Sensor 不代表正式 Pipeline 一定需要 Sensor。

---

# 19. Branching 現在需要嗎？

目前 Python Pipeline 已經可以處理：

```text
Non-Trading Day
↓
Empty DataFrame
↓
正常結束
↓
Loaded Rows = 0
```

因此沒有必要只是為了週末：

```text
Branch
├─ Process
└─ Skip
```

除非未來真的有不同 Business Flow。

所以：

```text
Branching
→ 需求導向
```

---

# 20. TaskGroup 現在需要嗎？

目前正式 DAG 主要只有：

```text
run_twse_pipeline
```

一個 Task。

因此：

```text
TaskGroup
→ 沒有必要
```

TaskGroup 應該在：

```text
Task 數量增加
↓
Graph 難閱讀
↓
需要 Logical Group
```

時再使用。

---

# 21. XCom 現在需要嗎？

目前：

```text
run_pipeline()
```

內部直接傳 DataFrame。

沒有真正：

```text
Cross-Task Communication
```

需求。

因此：

```text
XCom
→ 暫時不需要
```

未來拆 Task：

```text
Extract
↓
S3
↓
Transform
```

才可能使用 XCom：

```text
XCom
→ 傳 S3 URI
```

---

# 22. Sensor / TaskGroup / Branching 是否學會就全部加入？

不是。

正確觀念：

> **根據實際 Pipeline 的需求與應用場景選擇 Airflow Feature。**

例如：

```text
需要等待 External Condition
→ Sensor

需要 Conditional Flow
→ Branching

Task 太多需要整理
→ TaskGroup

Task 之間需要傳小型 Metadata
→ XCom

Temporary Failure
→ Retry
```

而不是：

```text
我會 Sensor
↓
加入 Sensor

我會 Branch
↓
加入 Branch

我會 TaskGroup
↓
加入 TaskGroup
```

這會造成：

```text
Over-engineering
```

---

# 23. 目前正式 Architecture

目前合理架構：

```text
              Airflow
                 ↓
          Daily Schedule
                 ↓
      Logical Date / Interval
                 ↓
       run_twse_pipeline
          │          │
          │          ├─ Retry
          │          ├─ Retry Delay
          │          └─ Timeout
          ↓
      run_pipeline(date)
                 ↓
              Extract
                 ↓
             Transform
                 ↓
             Validate
                 ↓
               Load
                 ↓
            PostgreSQL

Final Failure
      ↓
on_failure_callback
      ↓
Logging / Future Alert
```

目前暫時不需要：

```text
Sensor
TaskGroup
Branching
XCom
```

除非未來出現真正需求。

---

# 24. 未來可能的 Architecture

加入 Object Storage 後：

```text
              Airflow
                 ↓
            Extract Task
                 ↓
              S3 Raw
                 │
           XCom: S3 URI
                 ↓
           Transform Task
                 ↓
           S3 Processed
                 │
           XCom: S3 URI
                 ↓
           Validate Task
                 ↓
             Load Task
                 ↓
         PostgreSQL / DWH
```

這時：

```text
Task Boundary
```

才有更充分的理由重新設計。

---

# 25. Day 28 六題正式答案

## Q1：為什麼目前 TWSE Pipeline 維持一個 Airflow Task？

因為目前 ETL 流程相對單純，而且 Extract、Transform、Validate、Load 之間直接在同一個 Python Process 中傳遞 DataFrame。

如果現在拆成不同 Airflow Tasks，就需要額外處理跨 Task 的資料交換；大型 DataFrame 又不適合直接使用 XCom 傳遞。

目前尚未建立 S3 / Object Storage 等 External Storage Layer，因此維持：

```text
One Airflow Task
↓
run_pipeline()
```

可以避免不必要的架構複雜度。

---

## Q2：拆成不同 Tasks 最大的資料問題？

不同 Airflow Tasks 不能假設共享同一個 Process Memory。

因此：

```text
Extract
↓
DataFrame
↓
Transform
```

拆開後必須重新設計：

```text
Cross-Task Data Exchange
```

大型資料應該存到 External Storage，而不是直接塞進 XCom。

---

## Q3：為什麼不使用 XCom 傳大型 DataFrame？

因為 XCom 適合：

```text
Small Metadata
```

例如：

```text
trade_date
row_count
file_path
S3 URI
status
```

不適合：

```text
Large DataFrame
大量 Dataset
大型 Binary
```

因此：

```text
XCom
→ 傳資料在哪裡

External Storage
→ 存真正的資料
```

---

## Q4：加入 S3 後如何跨 Task 傳資料？

例如：

```text
Extract
↓
寫入 Parquet
↓
S3
↓
XCom 傳 S3 URI
↓
Transform
↓
從 S3 讀取資料
```

因此不是：

```text
Task A
↓
直接把大量資料傳給 Task B
```

而是：

```text
Task A
↓
External Storage
↑
Task B
```

XCom 只負責傳位置。

---

## Q5：Airflow Feature 是否學會就全部加入？

不是。

應該：

```text
Requirement
↓
Architecture Decision
↓
選擇適合的 Airflow Feature
```

而不是：

```text
Learn Feature
↓
一定加入 Production DAG
```

避免 Over-engineering。

---

## Q6：什麼是 Airflow Task Boundary？

> **決定一段工作在哪裡切成獨立 Airflow Task 的責任與執行邊界。**

Task Boundary 會影響：

```text
Execution
State
Retry
Timeout
Dependency
Monitoring
Failure Handling
Data Exchange
```

所以：

```text
Task 拆越細
≠
Architecture 越好
```

而是必須判斷：

```text
是否需要獨立 Retry？
是否需要獨立 Monitoring？
是否需要獨立 Timeout？
是否值得獨立執行？
資料能不能合理跨 Task 交換？
```

再決定 Task Boundary。

---

# Day 28 核心整理

今天最重要的 Architecture Pattern：

```text
目前

Airflow
   ↓
┌──────── Task ────────┐
│                      │
│ Extract              │
│ ↓                    │
│ Transform            │
│ ↓                    │
│ Validate             │
│ ↓                    │
│ Load                 │
│                      │
└──────────────────────┘
   ↓
PostgreSQL
```

未來：

```text
Extract Task
    ↓
S3 Raw
    │
    └── XCom：S3 URI
             ↓
       Transform Task
             ↓
       S3 Processed
             ↓
       Validate Task
             ↓
         Load Task
```

最後記住四句：

```text
Task Boundary
→ 決定工作在哪裡切成獨立 Airflow Task

XCom
→ 傳小型 Metadata / 資料位置

External Storage
→ 存放真正的大量資料

Airflow Feature
→ 根據 Requirement 使用，不是學會就全部加入
```

以及 Day 28 最重要的一句：

> **好的 Pipeline Architecture 不是使用最多技術，而是在正確的地方建立正確的 Boundary。**

## Day 28 完成

**Airflow Formal DAG Architecture Review / Task Boundary / Cross-Task Data Exchange ✅**
