# Day 6 學習筆記：新增 Data Validation 模組

今天在 `taiwan-finance-data-pipeline` 專案中新增 `src/validate.py`，目標是在資料進入 Load 階段、寫入 PostgreSQL 之前，先進行資料品質檢查。

原本的 ETL 流程是：

```text
extract → transform → load
```

今天新增 Validation 模組後，流程調整為：

```text
extract → transform → validate → load
```

這樣可以讓資料在寫入資料庫前，先確認內容是否合理，避免不合法資料進入 PostgreSQL。

## Validation 模組目標

`validate.py` 主要用於 Load 寫入前的資料檢查，並符合以下功能：

1. 檢查 DataFrame 是否為空。
2. 檢查交易量與價格欄位不可為負數。
3. 檢查 `high_price` 不應該小於 `low_price`。
4. 檢查 `stock_id` 與 `trade_date` 不可為空。
5. 將不合法資料過濾掉，並透過 warning log 記錄被排除的資料筆數。
6. 將 `validate.py` 模組整合進 `main.py`，讓主流程可以在 Transform 後、Load 前執行資料驗證。

## 今日完成事項

今天完成 `src/validate.py` 的建立，並將 Validation 流程嵌入到 `main.py` 中。

現在 Pipeline 執行時，會依序完成資料擷取、資料清洗、資料驗證與資料寫入。透過 Validation 層，可以在資料進入資料庫前先排除明顯錯誤的資料，例如負數價格、負數交易量、最高價低於最低價，或缺少 `stock_id` / `trade_date` 的資料。
