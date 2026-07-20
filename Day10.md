# Day 10 學習筆記：將 TWSE API 整合進正式 ETL Pipeline

今天將前兩天完成的 TWSE API 測試與資料轉換邏輯，正式整合進原有的 ETL Pipeline。

目前完整流程為：

```text
TWSE API
→ Extract
→ Transform
→ Validate
→ Load
→ PostgreSQL
```

## 1. Extract 改用 TWSE API

將原本從 CSV 讀取資料的方式，改為透過 TWSE API 取得每日股價原始資料，並使用 `fields9` 與 `data9` 建立 raw DataFrame。

透過這次調整，Pipeline 的資料來源正式從靜態測試資料升級為真實公開金融資料。

## 2. Transform 輸出標準八欄 Schema

將 TWSE 回傳的中文欄位轉換成專案內部使用的標準欄位：

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

透過統一內部 schema，可以讓後續的 Validation 與 Load 不需要了解 TWSE 原始資料格式。

## 3. 在 main.py 串接 TWSE Extraction

修改 `main.py`，將 TWSE API extraction 與 transformation 接入正式 ETL 流程。

`main.py` 負責依序執行：

```text
extract
→ transform
→ validate
→ load
```

因此各模組仍然維持明確的職責分工。

## 4. 資料沿用現有 Validation

轉換後的資料會進入現有的 Validation layer，檢查：

- 價格是否為負數
- 成交量是否為負數
- `high_price` 是否小於 `low_price`
- `stock_id` 是否為空
- `trade_date` 是否為空

這表示更換資料來源後，原本的資料品質檢查仍然可以繼續使用。

## 5. 驗證後資料寫入 PostgreSQL

通過 Validation 的資料會進入 Load 階段，並寫入 PostgreSQL。

Load 階段仍保留原有功能：

- Transaction control
- 發生錯誤時 rollback
- Duplicate handling
- 實際寫入筆數紀錄

## 6. 驗證 Pipeline 可以安全重跑

同一個交易日期執行第二次時，不會重複寫入相同資料。

原因是資料表使用：

```text
stock_id + trade_date
```

作為 unique constraint。

因此，這個 Pipeline 具備 idempotency，也就是同一批資料重跑後，不會產生重複結果。

## 7. 非交易日不會讓 Pipeline 異常中斷

當查詢日期為週末、國定假日或其他非交易日時，TWSE API 可能不會回傳股價資料。

目前程式會：

```text
取得空資料
→ 回傳空 DataFrame
→ 記錄 warning
→ 安全結束流程
```

而不是因為沒有資料就直接發生未處理的例外。

## 8. 使用 Row Count Logging 觀察資料流

目前日誌可以看到各階段的資料筆數：

```text
Extracted rows
Transformed rows
Validated rows
Loaded rows
```

透過比較各階段筆數，可以判斷：

- API 實際回傳多少資料
- Transform 是否有遺失資料
- Validation 排除了多少異常資料
- 最後實際寫入多少資料

這能提升 Pipeline 的可觀察性，也方便日後除錯。

## 今日理解

今天不只是將 API 接入程式，而是完成了一次正式的資料來源整合。

原本的 CSV MVP 已經升級為使用真實 TWSE 資料的 ETL Pipeline，同時保留既有的模組化設計、資料驗證、交易控制、重複資料處理與日誌紀錄。

整體架構如下：

```text
外部真實資料來源
→ 原始資料擷取
→ 內部標準格式轉換
→ 資料品質驗證
→ 可靠寫入資料庫
```

這次整合也驗證了模組化設計的價值：當資料來源從 CSV 改為 API 時，Validation 與 Load 大多可以直接沿用，不需要重新設計整個 Pipeline。
