# Day 14 學習筆記：建立 PostgreSQL Integration Test

今天建立了獨立的 PostgreSQL 測試環境，並使用 `pytest` 為 Load 模組撰寫 Integration Test。

測試流程為：

```text
固定測試 DataFrame
→ 呼叫 Load 函式
→ 寫入測試 PostgreSQL
→ 查詢資料庫
→ 驗證實際寫入結果
```

## 1. 建立獨立的 PostgreSQL 測試環境

測試使用獨立的 PostgreSQL 資料庫，不直接操作平常開發使用的資料庫。

這樣可以避免：

- 測試資料污染開發資料
- 清除測試資料時誤刪正式資料
- 不同測試互相影響
- 測試結果受到既有資料干擾

測試資料庫與開發資料庫使用不同的連線設定與連接埠，確保兩個環境彼此隔離。

## 2. 使用 pytest 測試 Load 模組

建立 Load Integration Test，將固定的小型 DataFrame 傳入現有 Load 函式。

測試不只確認 Load 函式沒有發生錯誤，也會直接查詢 PostgreSQL，確認資料確實已經寫入資料表。

這與先前的 Unit Test 不同：

```text
Unit Test
→ 測試單一函式的輸入與輸出

Integration Test
→ 測試 Python、SQLAlchemy、Load 模組與 PostgreSQL 是否能一起正常運作
```

## 3. 驗證寫入後的資料內容

資料寫入後，測試會直接查詢 PostgreSQL，確認以下欄位與測試輸入一致：

```text
stock_id
stock_name
trade_date
open_price
high_price
low_price
close_price
volume
```

這可以驗證：

- DataFrame 欄位能否正確對應資料表欄位
- Python 資料型別能否正確寫入 PostgreSQL
- Transaction 是否成功提交
- 寫入後的內容是否符合預期

## 4. 測試重複執行不會產生重複資料

將相同的測試 DataFrame 寫入兩次，再查詢資料庫中的實際筆數。

由於資料表使用：

```text
stock_id + trade_date
```

作為唯一條件，因此相同資料重跑後，資料庫仍然只會保留一筆紀錄。

這項測試驗證了 Pipeline 的 Idempotency：

> 相同輸入重複執行，不會產生重複的最終資料。

## 5. 每個測試前後清理資料表

使用 pytest fixture，在每個測試執行前後清理測試資料表。

測試流程為：

```text
測試開始前清除資料
→ 執行測試
→ 驗證結果
→ 測試結束後再次清除資料
```

這可以確保每個測試都從乾淨的資料庫狀態開始，不會依賴其他測試留下的資料。

因此，即使改變測試執行順序或重複執行測試，結果仍然應該保持一致。

## 今日理解

今天從原本只測純 Python 函式的 Unit Test，進一步學習如何測試程式與真實資料庫之間的整合。

目前測試範圍已包含：

```text
日期與 Transform 邏輯
→ Unit Test

Load 與 PostgreSQL 整合
→ Integration Test
```

Integration Test 可以驗證單元測試無法確認的問題，例如：

- 資料庫連線是否正常
- SQL 與資料表欄位是否相容
- Transaction 是否成功
- Unique Constraint 是否生效
- Duplicate Handling 是否符合預期
- 實際寫入內容是否正確

透過獨立測試資料庫與測試前後的資料清理，可以避免測試影響開發資料，並確保各個測試案例彼此獨立。
