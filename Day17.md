# Day 17：Airflow 每日排程、Data Interval 與 Retry

## 一、今日學習目標

今天將 Day 16 的手動觸發 DAG，改造成具備每日排程能力的 Airflow Pipeline。

Day 16 的執行方式：

```text
手動 Trigger DAG
→ 手動輸入 trade_date
→ Airflow Task 呼叫 run_pipeline(trade_date)
```

Day 17 的目標：

```text
Airflow 每日建立 DAG Run
→ 根據 Data Interval 決定資料日期
→ 產生 YYYYMMDD 格式的 trade_date
→ 呼叫 run_pipeline(trade_date)
→ 執行完整 ETL
```

同時加入 Retry，讓暫時性的 API 或資料庫錯誤可以由 Airflow 自動重試。

---

## 二、今日架構

修改後的架構仍然維持單一 Airflow Task：

```text
twse_daily_pipeline DAG
│
└── run_twse_pipeline Task
        │
        └── run_pipeline(trade_date)
                ├── Extract
                ├── Transform
                ├── Validate
                └── Load
```

今天沒有將 ETL 拆成：

```text
extract_task
→ transform_task
→ validate_task
→ load_task
```

Airflow 負責排程與執行管理，既有 `run_pipeline()` 仍負責完整 ETL。

---

## 三、每日排程

Day 15 使用：

```python
schedule=None
```

代表沒有自動排程，只能手動觸發。

Day 16 修改為：

```python
schedule="@daily"
```

代表 Airflow 會根據每日排程建立 DAG Run。

目前仍保留：

```python
catchup=False
```

避免 Airflow 根據過去的 `start_date`，一次建立大量歷史 DAG Run。

目前設定：

```text
schedule="@daily"
catchup=False
```

代表：

```text
從目前開始建立每日排程
但不自動補跑 start_date 到現在之間的歷史區間
```

---

## 四、Schedule 不只是執行時間

Airflow 的排程不只是設定「幾點執行」，也會建立該次 DAG Run 所負責的資料區間。

例如每日排程的資料區間可能是：

```text
Data Interval Start：2026-08-03 00:00
Data Interval End：2026-08-04 00:00
```

這代表該次 DAG Run 負責處理：

```text
2026-08-03 這一天的資料
```

因此，排程與資料日期會被明確綁定。

---

## 五、Logical Date

Logical Date 是 Airflow 用來表示某次 DAG Run 邏輯時間的欄位。

它不是 Task 真正開始執行的時間。

例如：

```text
Task 實際執行時間：2026-08-04 01:00
該次執行負責的資料：2026-08-03
```

因此：

```text
實際執行時間
≠
資料所屬日期
```

Logical Date 主要用來表示該次 DAG Run 在資料處理上的時間意義。

---

## 六、Data Interval

Data Interval 代表某次 DAG Run 所負責的資料時間區間。

每日排程的概念：

```text
data_interval_start
→ 該批次資料區間的開始時間

data_interval_end
→ 該批次資料區間的結束時間
```

例如：

```text
data_interval_start：2026-08-03 00:00
data_interval_end：2026-08-04 00:00
```

對每日股價 Pipeline 而言，可以使用：

```python
data_interval_start
```

所代表的日期作為 `trade_date`。

轉換方式：

```python
trade_date = data_interval_start.strftime("%Y%m%d")
```

結果：

```text
2026-08-03 00:00
→
20260803
```

再傳入：

```python
run_pipeline("20260803")
```

---

## 七、為什麼不能使用 datetime.now()

不應使用：

```python
datetime.now().strftime("%Y%m%d")
```

也不應使用：

```python
date.today()
```

來決定 ETL 要處理的資料日期。

原因是 Task 可能發生：

```text
排程延遲
Worker 排隊
Task Retry
手動重跑
歷史執行
跨日執行
```

例如原本應處理：

```text
2026-08-03
```

但 Task 因為失敗，在：

```text
2026-08-04
```

重新執行。

若使用 `datetime.now()`，就可能錯誤處理：

```text
2026-08-04
```

但使用 `data_interval_start`，該次 DAG Run 仍會處理：

```text
2026-08-03
```

重要理解：

```text
datetime.now()
→ 現在的系統時間

data_interval_start
→ 這次批次應該負責的資料日期
```

---

## 八、日期來源的優先順序

Day 16 同時保留手動指定日期與自動排程日期。

日期解析順序：

```text
有手動輸入 trade_date
→ 使用手動日期

沒有手動輸入 trade_date
→ 使用 data_interval_start
```

這樣同一個 DAG 可以支援：

```text
每日排程
特定日期重跑
手動測試
資料修復
```

---

## 九、手動日期參數

手動 Trigger DAG 時，可以輸入：

```json
{
  "trade_date": "20240701"
}
```

Task 會優先使用：

```text
20240701
```

而不使用 Data Interval 的日期。

用途包括：

```text
測試特定日期
重新執行某一天
修補遺漏資料
驗證歷史資料
```

若沒有輸入 `trade_date`，Task 才會使用：

```python
context["data_interval_start"]
```

---

## 十、Airflow Params

Day 16 使用 Airflow Param 定義可選的 `trade_date`。

概念設定：

```python
params={
    "trade_date": Param(
        default=None,
        type=["null", "string"],
        pattern=r"^\d{8}$",
        description="Optional trade date in YYYYMMDD format",
    )
}
```

用途：

```text
定義參數名稱
設定預設值
限制輸入格式
提供手動 Trigger 使用
```

`pattern`：

```text
^\d{8}$
```

代表只能輸入八位數字，例如：

```text
20240701
```

但要注意，這只能驗證格式是八位數字，不一定能驗證日期真的存在。

例如：

```text
20240230
```

仍可能符合八位數格式。

真正的日期合法性仍由現有 `run_pipeline()` 或日期驗證函式負責。

---

## 十一、Task Context

Task 執行時，可以透過：

```python
context = get_current_context()
```

取得該次執行相關資訊。

例如：

```text
params
dag_run
logical_date
data_interval_start
data_interval_end
task_instance
```

Day 16 主要使用：

```python
context["params"]
```

以及：

```python
context["data_interval_start"]
```

概念程式：

```python
context = get_current_context()

manual_trade_date = context["params"].get("trade_date")

if manual_trade_date:
    trade_date = manual_trade_date
else:
    trade_date = context[
        "data_interval_start"
    ].strftime("%Y%m%d")
```

---

## 十二、日期來源 Logging

為了讓執行結果更容易追蹤，Task 會記錄日期來源。

概念：

```python
if manual_trade_date:
    trade_date = manual_trade_date
    date_source = "manual parameter"
else:
    trade_date = context[
        "data_interval_start"
    ].strftime("%Y%m%d")
    date_source = "data interval"

print(
    f"Resolved trade_date={trade_date}, "
    f"source={date_source}"
)
```

手動輸入時，Log 可能顯示：

```text
Resolved trade_date=20240701, source=manual parameter
```

未輸入日期時，Log 可能顯示：

```text
Resolved trade_date=20260803, source=data interval
```

透過 Log 可以確認：

```text
本次 ETL 處理哪個日期
日期來自手動輸入還是排程區間
```

---

## 十三、Retry

Retry 是 Task 失敗後，由 Airflow 自動重新執行。

Day 16 設定：

```python
default_args={
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
```

代表：

```text
第一次正常執行失敗
→ 等待 5 分鐘
→ Retry 第一次

如果仍然失敗
→ 再等待 5 分鐘
→ Retry 第二次
```

總共最多執行：

```text
初始執行 1 次
＋
Retry 2 次
＝
最多 3 次
```

---

## 十四、Retry 由 Airflow 管理

不需要在 Task 中自行撰寫：

```python
for attempt in range(3):
    try:
        ...
    except Exception:
        ...
```

也不需要使用：

```python
time.sleep()
```

Airflow 會負責：

```text
記錄失敗
設定 Task 為 up_for_retry
等待 retry_delay
重新排程 Task
重新交給 Worker 執行
```

這樣 Airflow UI 才能清楚顯示每次 Retry 狀態。

---

## 十五、適合 Retry 的錯誤

Retry 適合處理暫時性錯誤，例如：

```text
TWSE API timeout
網路短暫中斷
PostgreSQL 暫時無法連線
DNS 暫時解析失敗
服務短暫不可用
Connection reset
```

這些錯誤重新執行後，有可能恢復正常。

---

## 十六、不適合 Retry 的錯誤

以下錯誤通常不會因為 Retry 自動修復：

```text
日期格式錯誤
不存在的日期
Python 語法錯誤
缺少 Python 套件
Import 路徑錯誤
帳號密碼錯誤
資料庫 Schema 錯誤
程式邏輯錯誤
必要欄位缺失
```

例如日期輸入：

```text
20240230
```

即使重試兩次，仍然是不合法日期。

重要理解：

```text
Retry 用來處理暫時性錯誤，
不是用來掩蓋設定或程式錯誤。
```

---

## 十七、Retry 與 Idempotency

Airflow Retry 會重新執行 Task。

因此 ETL 必須具備 Idempotency，否則 Retry 可能造成重複寫入。

目前專案已使用：

```text
stock_id + trade_date
```

作為 Unique Constraint。

因此相同資料因 Retry 或手動重跑再次寫入時，不會產生重複資料。

完整關係：

```text
Task 執行失敗
→ Airflow Retry
→ run_pipeline() 再次執行
→ 相同資料再次嘗試寫入
→ Unique Constraint 防止重複
```

因此：

```text
Retry
＋
Idempotency
```

是資料批次系統中需要同時具備的能力。

---

## 十八、Timezone

Airflow 排程會受到 Timezone 設定影響。

假設 Airflow 使用 UTC：

```text
@daily
→ 每天 UTC 00:00 建立排程
```

換算台北時間：

```text
UTC 00:00
→ 台北時間 08:00
```

因此，看到：

```python
schedule="@daily"
```

不能直接假設一定是台北時間凌晨 00:00。

還需要確認：

```text
Airflow default timezone
DAG start_date timezone
Airflow UI 顯示 timezone
```

Day 16 先理解 Timezone 會影響實際排程時間，不急著設定特定業務執行時刻。

---

## 十九、今日 DAG 概念程式

```python
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, get_current_context, task

from src.main import run_pipeline


with DAG(
    dag_id="twse_daily_pipeline",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "trade_date": Param(
            default=None,
            type=["null", "string"],
            pattern=r"^\d{8}$",
            description="Optional trade date in YYYYMMDD format",
        )
    },
    tags=["twse", "data-engineering"],
) as dag:

    @task
    def run_twse_pipeline() -> int:
        context = get_current_context()

        manual_trade_date = context["params"].get(
            "trade_date"
        )

        if manual_trade_date:
            trade_date = manual_trade_date
            date_source = "manual parameter"
        else:
            trade_date = context[
                "data_interval_start"
            ].strftime("%Y%m%d")
            date_source = "data interval"

        print(
            f"Resolved trade_date={trade_date}, "
            f"source={date_source}"
        )

        return run_pipeline(trade_date)

    run_twse_pipeline()
```

實際程式需依目前 Airflow 版本、`run_pipeline()` 回傳型別與現有 DAG 結構調整。

---

## 二十、完整執行流程

排程執行時：

```text
1. Airflow Scheduler 判斷每日排程時間到達
2. 建立 Scheduled DAG Run
3. 產生該次 Data Interval
4. Scheduler 將 Task 排入執行
5. Worker 執行 run_twse_pipeline
6. Task 取得 data_interval_start
7. 將日期轉成 YYYYMMDD
8. 呼叫 run_pipeline(trade_date)
9. Extract 呼叫 TWSE API
10. Transform 清洗資料
11. Validate 驗證資料品質
12. Load 寫入 PostgreSQL
13. 成功時 Task 顯示 success
14. 暫時性失敗時進入 Retry
```

手動執行時：

```text
1. 使用者手動 Trigger DAG
2. 輸入 trade_date
3. Task 優先取得手動參數
4. 呼叫 run_pipeline(trade_date)
5. 執行完整 ETL
```

---

## 二十一、今日驗證方式

### 測試一：手動指定日期

手動 Trigger：

```json
{
  "trade_date": "20240701"
}
```

Task Log 應顯示：

```text
Resolved trade_date=20240701, source=manual parameter
```

並完成：

```text
Extract
Transform
Validate
Load
```

---

### 測試二：未指定日期

手動 Trigger 時將 `trade_date` 留空。

Task 應改用：

```text
data_interval_start
```

Log 應顯示：

```text
Resolved trade_date=YYYYMMDD, source=data interval
```

正式每日批次的日期語意，應以 Scheduled DAG Run 的 Data Interval 為準。

---

### 測試三：確認 Retry 設定

在 Airflow UI 中確認 Task 設定：

```text
Retries：2
Retry Delay：5 minutes
```

Task 遇到暫時性錯誤時，狀態可能依序為：

```text
running
→ up_for_retry
→ scheduled
→ queued
→ running
```

不要為了測試 Retry 而故意修改正式資料庫密碼或破壞正式設定。

---

## 二十二、今天沒有修改的內容

Day 16 不修改：

```text
Extract 邏輯
Transform 邏輯
Validate 規則
Load 邏輯
run_pipeline() 商業邏輯
PostgreSQL Schema
Unique Constraint
Unit Test
Integration Test
```

也沒有：

```text
將 ETL 拆成多個 Task
開啟 catchup=True
執行大量 Backfill
使用 XCom 傳遞 DataFrame
加入失敗通知
```

---

## 二十三、今日重要理解

### 每日排程

```text
schedule="@daily"
代表 Airflow 根據每日資料區間建立 DAG Run。
```

### Logical Date

```text
Logical Date 表示該次 DAG Run 的邏輯時間，
不一定等於 Task 實際開始時間。
```

### Data Interval

```text
Data Interval 表示該次批次負責的資料區間。
```

### 資料日期

```text
正式排程應使用 data_interval_start 決定 trade_date，
不應使用 datetime.now()。
```

### 手動參數

```text
手動 trade_date 優先於 Data Interval，
可用於測試、修補與特定日期重跑。
```

### Retry

```text
Retry 用於處理暫時性失敗，
不是修正程式或設定錯誤。
```

### Retry 與 Idempotency

```text
Retry 可能重新執行相同資料，
因此 Pipeline 必須具備 Idempotency。
```

### Timezone

```text
@daily 的實際觸發時間取決於 Airflow Timezone，
不能只看排程字串判斷台北執行時間。
```

---

## 二十四、Day 16 完成狀態

```text
[✓] 將 schedule=None 改為每日排程
[✓] 維持 catchup=False
[✓] 理解 Logical Date
[✓] 理解 Data Interval
[✓] 使用 data_interval_start 產生 trade_date
[✓] 未使用 datetime.now() 決定資料日期
[✓] 保留手動 trade_date 參數
[✓] 設定 retries=2
[✓] 設定 retry_delay=5 分鐘
[✓] 理解適合與不適合 Retry 的錯誤
[✓] 理解 Retry 與 Idempotency 的關係
[✓] 理解 Timezone 對排程時間的影響
[✓] 沒有拆分既有 ETL Task
```

---

## 二十五、下一步

Day 17 可以進入：

```text
Airflow Catchup
＋
Backfill
＋
歷史資料補跑
```

接下來會理解：

```text
catchup=True 會發生什麼
Scheduled Run 與 Manual Run 的差異
如何安全回補歷史日期
Airflow Backfill 與現有日期區間模式的差異
如何避免大量歷史 DAG Run 同時執行
```

下一階段的重點不是立刻開啟大量 Catchup，而是先理解 Airflow 如何管理歷史資料區間，再設計安全的 Backfill 策略。
