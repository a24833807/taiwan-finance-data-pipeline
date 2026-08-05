# Day 18：Airflow Catchup、Backfill 與 Idempotency

## 一、Catchup

Catchup 是 Airflow 根據 DAG 的：

```text
start_date
＋
schedule
```

自動補齊過去尚未建立的排程區間。

例如：

```text
start_date：2026-08-01
schedule：@daily
目前日期：2026-08-06
catchup：True
```

Airflow 可能依照每日排程，建立過去尚未執行的 DAG Runs：

```text
2026-08-01
2026-08-02
2026-08-03
2026-08-04
2026-08-05
```

因此，更精確的理解是：

```text
Catchup
→ 從 start_date 開始，
依照 DAG 的 schedule，
自動補齊過去缺少的排程資料區間。
```

Catchup 不是只執行一次涵蓋多天的批次，而是可能建立多個獨立 DAG Runs。

例如每日排程會變成：

```text
一個日期
→ 一個 DAG Run
```

目前專案仍維持：

```python
catchup=False
```

原因是避免 Airflow 一次自動產生大量歷史執行，造成 TWSE API、Worker 和 PostgreSQL 的負擔。

---

## 二、Backfill

Backfill 是由使用者明確指定一段歷史日期範圍，要求 Airflow 建立並執行該範圍內的歷史 DAG Runs。

例如指定：

```text
2024-07-01
到
2024-07-03
```

若 DAG 使用每日排程，Airflow 會建立：

```text
2024-07-01 DAG Run
2024-07-02 DAG Run
2024-07-03 DAG Run
```

每個 DAG Run 都會使用自己的：

```text
Logical Date
Data Interval
Task 狀態
Task Log
```

因此，更精確的理解是：

```text
Backfill
→ 由使用者明確指定歷史時間範圍，
依照 DAG 的 schedule 建立該範圍內的歷史 DAG Runs。
```

Backfill 不等於一個 Task 內部跑日期迴圈。

---

## 三、Catchup 與 Backfill 的差異

```text
Catchup
→ Scheduler 自動補齊缺少的歷史排程區間

Backfill
→ 使用者主動指定歷史日期範圍執行
```

例如：

```text
Catchup：
系統發現從 start_date 到現在有缺少的每日排程，
因此自動建立歷史 DAG Runs。

Backfill：
使用者指定 2024-07-01 到 2024-07-03，
要求 Airflow 補跑這三天。
```

目前採取的安全策略是：

```text
catchup=False
＋
需要補資料時才明確執行短區間 Backfill
```

這樣可以避免無意間建立大量歷史任務。

---

## 四、Airflow Backfill 與 CLI 區間模式

目前專案本身也支援日期區間：

```bash
python src/main.py \
  --start-date 20240701 \
  --end-date 20240703
```

兩者的差異如下。

### CLI 日期區間模式

```text
一次程式執行
→ 程式內部逐日迴圈
→ 處理多個日期
```

概念：

```text
一個 Process
一份整批 Log
內部處理多天
```

### Airflow Backfill

```text
指定日期區間
→ 每一天建立獨立 DAG Run
→ 每一天有獨立 Task 與 Log
```

概念：

```text
2024-07-01 → 一個 DAG Run
2024-07-02 → 一個 DAG Run
2024-07-03 → 一個 DAG Run
```

Airflow Backfill 的優點是每一天可以獨立：

```text
成功
失敗
Retry
監控
重新執行
```

---

## 五、為什麼歷史補跑仍需要 Idempotency？

Idempotency 是指：

```text
相同輸入重複執行，
不會產生錯誤或重複結果。
```

歷史補跑可能因為多種原因重複執行同一天：

```text
Backfill 被重複下指令
某一天 Task Retry
失敗後手動重跑
使用不同 reprocess behavior
同一天曾經由 CLI 執行
同一天曾經由 Airflow 排程執行
```

例如：

```text
第一次執行 2024-07-01
→ 寫入 1,000 筆資料

再次 Backfill 2024-07-01
→ 又執行相同 ETL
```

如果 Pipeline 不具備 Idempotency，可能變成：

```text
第一次：1,000 筆
第二次：再新增 1,000 筆
結果：2,000 筆重複資料
```

目前專案使用：

```text
stock_id + trade_date
```

作為 Unique Constraint。

因此同一檔股票、同一交易日只能存在一筆資料。

預期行為：

```text
第一次執行
→ 寫入資料

第二次執行相同日期
→ 不產生重複資料
```

所以 Idempotency 是 Backfill 的安全基礎。

---

## 六、Backfill 與 Idempotency 的關係

完整關係如下：

```text
Airflow Backfill
→ 可能重新處理歷史日期
→ 相同日期可能執行不只一次
→ Pipeline 必須允許安全重跑
→ Unique Constraint 防止重複資料
```

也就是：

```text
Backfill 負責重新執行歷史批次。

Idempotency 負責確保歷史批次重跑仍然安全。
```

Airflow 可以決定要不要建立新的 DAG Run，但資料庫層仍需要防止重複資料。

例如：

```text
--reprocess-behavior none
```

可以避免已有 DAG Run 時建立新的 Backfill Run。

但它不能完全取代資料庫的 Unique Constraint，因為資料仍可能透過：

```text
CLI
其他 DAG
Retry
手動程式
```

被再次寫入。

因此需要兩層保護：

```text
Airflow 執行層
→ 控制是否重建 DAG Run

資料庫資料層
→ Unique Constraint 防止重複資料
```

---

## 七、Dry Run

執行 Backfill 前，可以加入：

```text
--dry-run
```

Dry Run 是預演模式。

用途：

```text
先查看 Backfill 預計處理哪些歷史日期，
但不真正建立與執行 DAG Runs。
```

Dry Run 不會：

```text
執行 Task
呼叫 TWSE API
寫入 PostgreSQL
```

流程：

```text
先執行 Dry Run
→ 確認日期範圍正確
→ 移除 --dry-run
→ 正式執行 Backfill
```

---

## 八、歷史補跑的安全措施

歷史 Backfill 前應確認：

```text
1. 先使用 --dry-run 預覽日期
2. 使用短日期區間測試
3. 設定 max_active_runs=1
4. Backfill 使用 --max-active-runs 1
5. 不要傳入固定 trade_date
6. 每個 DAG Run 使用自己的 data_interval_start
7. 確認 Pipeline 具備 Idempotency
8. 確認非交易日可以正常結束
```

其中：

```python
max_active_runs=1
```

代表同一個 DAG 同時最多執行一個 DAG Run，避免大量歷史批次同時呼叫 TWSE API 與 PostgreSQL。

---

## 九、今日重要理解

### Catchup

```text
從 start_date 開始，
依照 schedule 自動補齊過去缺少的排程區間。
```

### Backfill

```text
由使用者明確指定歷史日期範圍，
建立並執行該範圍內的歷史 DAG Runs。
```

### Idempotency

```text
相同歷史日期重複執行時，
不會產生重複資料。
```

### 三者的關係

```text
Catchup
→ 自動補歷史批次

Backfill
→ 手動指定範圍補歷史批次

Idempotency
→ 確保歷史批次重跑仍然安全
```

---

## 十、Day 17 完成內容

```text
[✓] 理解 Catchup
[✓] 理解 Backfill
[✓] 理解 Catchup 與 Backfill 的差異
[✓] 理解 Airflow Backfill 與 CLI 區間模式的差異
[✓] 理解歷史補跑仍需要 Idempotency
[✓] 理解 Unique Constraint 是資料層防重複機制
[✓] 理解 Dry Run 是正式執行前的預演
[✓] 理解歷史補跑應限制同時執行數
[✓] catchup 仍維持 False
```
