# Day 26｜Airflow Sensor：等待外部條件

## 今日主題

今天學習 Airflow 的 **Sensor**。

前面的 Airflow 課程主要處理：

```text
Task 怎麼執行？
Task 失敗怎麼 Retry？
Task 最終失敗怎麼 Callback？
```

今天開始處理另一個 Data Pipeline 常見問題：

> 如果 Pipeline 已經啟動，但執行需要的外部條件還沒有 Ready，該怎麼辦？

例如：

```text
Pipeline 啟動
↓
等待上游檔案
↓
檔案還沒到
↓
不能開始 Process
```

這時就可以使用：

```text
Sensor
```

---

# 1. Sensor 是什麼？

Sensor 是 Airflow 中用來：

> **等待／檢查某個條件是否成立的特殊 Task。**

Sensor 的核心不是「傳遞訊號」，而是：

```text
Wait for something
```

例如：

```text
等待檔案出現
等待 Database 資料 Ready
等待上游 Pipeline 完成
等待外部條件成立
```

條件成立之後：

```text
Sensor Success
↓
Downstream Task
↓
繼續執行
```

---

# 2. 一般 Task 與 Sensor 的差異

一般 Task：

```text
Do something
```

例如：

```text
Extract
Transform
Validate
Load
```

Sensor：

```text
Wait for something
```

例如：

```text
等待 CSV 到達
等待上游資料 Ready
等待其他 DAG 完成
```

因此可以簡單理解：

```text
Normal Task
→ 做事情

Sensor
→ 等事情
```

---

# 3. Sensor 基本流程

Sensor 會持續檢查某個條件。

例如：

```text
檔案到了嗎？
↓
False
↓
等待
↓
再次檢查

檔案到了嗎？
↓
False
↓
等待
↓
再次檢查

檔案到了嗎？
↓
True
↓
Sensor Success
↓
Process Data
```

因此：

```text
False
≠
Failure
```

False 代表：

> 條件目前還沒有 Ready。

Sensor 會繼續等待。

---

# 4. Poke

Sensor 每次檢查條件，可以理解成一次：

```text
poke
```

白話就是：

> 「去看一下條件好了沒？」

例如：

```text
poke
↓
檔案到了嗎？
↓
No

等待

poke
↓
檔案到了嗎？
↓
No

等待

poke
↓
檔案到了嗎？
↓
Yes
```

當條件成立：

```text
Sensor Success
```

---

# 5. `poke_interval`

例如：

```python
poke_interval=5
```

代表：

> Sensor 每隔約 5 秒重新檢查一次條件。

流程：

```text
Check
↓
False
↓
等待 5 秒
↓
Check
↓
False
↓
等待 5 秒
↓
Check
```

所以：

```text
poke_interval
→ 多久重新檢查一次
```

---

# 6. `poke_interval=5` 不代表什麼？

它不是：

```text
最多檢查 5 次
```

也不是：

```text
最多等待 5 秒
```

而是：

```text
兩次條件檢查之間
大約間隔 5 秒
```

---

# 7. Sensor `timeout`

Sensor 不應該永遠等待。

例如：

```text
等待 daily_stock.csv
↓
一直沒有出現
↓
一直等
↓
一直等
↓
...
```

因此可以設定：

```python
timeout=20
```

代表：

> Sensor 最多允許等待條件成立約 20 秒。

如果條件一直：

```text
False
```

最後：

```text
Sensor Timeout
↓
Failed
```

---

# 8. `timeout=20` 不代表一定等待 20 秒

例如：

```text
0 秒
Check → False

5 秒
Check → False

10 秒
Check → True
```

條件第 10 秒已經成立：

```text
Sensor Success
```

就直接繼續。

不需要：

```text
繼續等到 20 秒
```

因此：

```text
timeout
→ 最多可以等多久
```

不是：

```text
固定要等多久
```

---

# 9. `poke_interval` 與 `timeout`

這兩個設定一定要分清楚。

例如：

```python
poke_interval=5
timeout=20
```

代表：

```text
poke_interval
→ 大約每 5 秒檢查一次

timeout
→ 最多允許等待約 20 秒
```

概念流程：

```text
Check
↓ False

等待 5 秒

Check
↓ False

等待 5 秒

Check
↓ False

...

超過 Timeout
↓
Sensor Failed
```

---

# 10. Sensor 條件 True

今天第一個實驗：

```text
Condition = True
```

流程：

```text
wait_for_condition
↓
Check
↓
True
↓
Sensor Success
↓
process_data
↓
Success
```

因此 Airflow UI 可以看到：

```text
wait_for_condition   Success
        ↓
process_data         Success
```

---

# 11. Sensor 條件 False

第二個實驗：

```text
Condition = False
```

Sensor 不會立即 Failure。

而是：

```text
Check
↓
False
↓
Not Ready
↓
等待 poke_interval
↓
再次 Check
```

因此最重要的觀念：

> **Not Ready ≠ Failure**

---

# 12. Sensor Timeout

如果：

```text
Condition
一直都是 False
```

而且超過：

```python
timeout=20
```

最後：

```text
wait_for_condition
↓
Timeout
↓
Failed
```

Downstream：

```text
process_data
```

因為 upstream Sensor 沒有 Success，在預設 Dependency / Trigger Rule 下不會正常執行。

概念：

```text
wait_for_condition
        ↓
      Failed
        ↓
process_data
   不執行
```

---

# 13. Sensor 與 Retry 的差異

這是 Day 26 最重要的觀念。

## Sensor

處理的是：

> **工作還不能開始，因為某個條件尚未 Ready。**

例如：

```text
檔案到了嗎？
↓
No
↓
不是 Error
↓
繼續等待
↓
檔案到了
↓
Yes
↓
Sensor Success
↓
開始 Process
```

---

## Retry

處理的是：

> **工作已經開始執行，但是發生 Failure。**

例如：

```text
開始呼叫 API
↓
Network Error
↓
Task Failure
↓
Retry
↓
重新執行
```

---

# 14. Sensor vs Retry 核心比較

```text
Sensor
→ 條件還沒 Ready
→ 等待

Retry
→ 工作已經 Failed
→ 重新嘗試
```

所以：

```text
Sensor False
≠
Task Failure
```

而：

```text
Retry
```

通常是在 Task 已經發生 Failure 後才介入。

最重要的一句：

> **Sensor 處理「Not Ready」，Retry 處理「Failed」。**

---

# 15. Sensor 不只是等待檔案

檔案只是其中一種應用。

Sensor 可以等待很多不同條件。

### File

```text
等待 CSV
↓
CSV 出現
↓
開始 ETL
```

### Database

```text
等待上游 Table 資料 Ready
↓
Ready
↓
開始 Transform
```

### External DAG / Pipeline

```text
等待 upstream_pipeline
↓
Upstream Success
↓
執行 downstream_pipeline
```

因此不要把：

```text
Sensor
```

記成：

```text
專門等待檔案
```

而應該記：

```text
Sensor
→ 等待某個條件成立
```

---

# 16. Sensor 與 Branching 的差異

之前學過 Branching。

Branch：

```text
判斷條件
↓
走哪一條？

      ┌→ Task A
Branch
      └→ Task B
```

Sensor：

```text
判斷條件
↓
現在能不能繼續？

False
↓
等待

True
↓
繼續
```

因此：

```text
Branch
→ 決定走哪條路

Sensor
→ 決定現在能不能繼續
```

---

# 17. Sensor 與 `execution_timeout`

Day 24 學過：

```python
execution_timeout
```

今天又看到：

```python
timeout
```

兩者不要混淆。

## `execution_timeout`

控制：

```text
單次 Task Attempt
最多允許執行多久
```

## Sensor `timeout`

控制：

```text
Sensor
最多允許等待條件多久
```

簡化記憶：

```text
execution_timeout
→ Task 執行時間限制

Sensor timeout
→ Sensor 等待時間限制
```

---

# 18. 今日 Demo DAG

新增：

```text
airflow/dags/sensor_demo_dag.py
```

DAG：

```text
sensor_demo
```

流程：

```text
wait_for_condition
        ↓
process_data
```

Sensor 設定：

```text
poke_interval = 5 秒

timeout = 20 秒
```

第一階段：

```text
Condition = True

wait_for_condition
→ Success

process_data
→ Success
```

第二階段：

```text
Condition = False

wait_for_condition
→ 持續等待
→ Timeout
→ Failed

process_data
→ 不執行
```

---

# 19. TaskFlow Sensor 概念

今天使用 TaskFlow Sensor API。

概念：

```python
@task.sensor(
    poke_interval=5,
    timeout=20,
)
def wait_for_condition():

    print("Checking condition...")

    return PokeReturnValue(
        is_done=True
    )
```

其中：

```text
is_done=True
```

代表：

```text
條件已成立
↓
Sensor Success
```

而：

```text
is_done=False
```

代表：

```text
條件尚未成立
↓
繼續等待
```

實際 import/API 應依目前專案使用的 Airflow 版本為準，不需要死背 import path。

---

# 20. 套用到 Data Pipeline

假設未來有一個 Batch Pipeline：

```text
上游系統
↓
產生資料
↓
你的 Pipeline
```

可以設計：

```text
Airflow Schedule
      ↓
Sensor
      ↓
資料 Ready？
├── No
│    ↓
│  等待
│
└── Yes
     ↓
   Extract
     ↓
   Transform
     ↓
   Validate
     ↓
   Load
```

這比：

```text
資料還沒到
↓
Process 直接執行
↓
Error
↓
Retry
```

更符合真正的流程語意。

因為：

> 資料還沒到不一定是系統錯誤，只是執行條件尚未成立。

---

# 21. Day 24～26 串聯

現在已經可以把前幾天的 Failure Handling 與 Sensor 串起來。

```text
Sensor
│
├── Not Ready
│      ↓
│    等待
│
└── Ready
       ↓
    Task 執行
       ↓
   執行成功？
   │
   ├── Yes
   │     ↓
   │   Success
   │
   └── No
         ↓
      Failure
         ↓
     還有 Retry？
      │       │
     Yes      No
      │       │
retry_delay   │
      ↓       │
New Attempt   │
              ↓
         Final Failed
              ↓
     on_failure_callback
              ↓
        Logging / Alert
```

因此目前已經具備：

```text
Sensor
→ 執行前條件控制

execution_timeout
→ 單次 Attempt 執行時間控制

retries
→ Failure 後重新嘗試

retry_delay
→ Retry 間隔

on_failure_callback
→ 最終 Failure 後處理
```

---

# 22. 今日實際學會

## 1. Sensor 是拿來做什麼？

Sensor 用來：

> 等待／檢查某個外部條件是否成立。

例如：

```text
等待檔案
等待 DB 資料
等待上游 Pipeline
```

條件成立後，Downstream Task 才繼續。

---

## 2. `poke_interval=5`

代表：

> 大約每 5 秒重新檢查一次條件。

---

## 3. `timeout=20`

代表：

> Sensor 最多允許等待條件成立約 20 秒。

如果條件提早成立，就會直接 Success，不需要等待滿 20 秒。

---

## 4. Sensor 回傳 False

代表：

```text
Not Ready
```

不是：

```text
Failure
```

因此：

```text
False
↓
等待 poke_interval
↓
重新檢查
```

---

## 5. Sensor 最終 Timeout

如果條件一直沒有成立：

```text
Sensor
↓
Timeout
↓
Failed
```

在預設 Dependency / Trigger Rule 下：

```text
Downstream Task
→ 不會正常執行
```

---

## 6. Sensor vs Retry

最重要的差異：

```text
Sensor
→ 條件還沒 Ready
→ 等待

Retry
→ 工作已經 Failed
→ 重新嘗試
```

也就是：

> **Sensor 處理 Not Ready；Retry 處理 Failed。**

---

# Day 26 核心整理

```text
Pipeline Start
      ↓
    Sensor
      ↓
Condition Ready？
 │           │
No          Yes
 │           │
等待         ↓
 │        Success
 │           ↓
再次檢查   Process
 │           ↓
 │        Task 執行
 │
 └─ 超過 timeout
          ↓
        Failed
```

三個最重要概念：

```text
Sensor
→ 等待條件成立

poke_interval
→ 多久檢查一次

timeout
→ 最多允許等待多久
```

以及 Day 26 最重要的一句：

```text
Not Ready ≠ Failure

Sensor
→ Wait

Retry
→ Try Again After Failure
```

## Day 26 完成

**Airflow Sensor / External Condition Waiting ✅**
