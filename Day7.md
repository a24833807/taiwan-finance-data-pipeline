# Day 7 學習日誌：第一週 MVP 收斂與 End-to-End 測試

今天主要針對 `taiwan-finance-data-pipeline` 專案進行第一週成果整理，並完成 MVP 版本的 End-to-End 測試。

本次測試包含三種情境：

1. **正常資料測試**
   確認 CSV 資料可以正常經過 Extract、Transform、Validate 與 Load 流程，最後成功寫入 PostgreSQL。

2. **重複資料測試**
   針對相同資料重複執行 ETL，確認 `stock_id` 與 `trade_date` 組成的 unique key 可以避免重複資料寫入。

3. **錯誤資料測試**
   透過有問題的測試資料，確認 Validation 層可以在 Load 前進行資料檢查，避免錯誤資料被寫入 PostgreSQL。

這是我第一週完成的 MVP 版本。專案目前使用 CSV 作為資料來源，透過 Python 執行 ETL 流程，包含資料擷取、資料清洗、資料驗證與資料寫入，最後將通過檢查的資料載入 PostgreSQL。

在架構設計上，我將 ETL 流程拆分成多個模組，包含 `extract.py`、`transform.py`、`validate.py`、`load.py`、`config.py` 與 `db.py`，讓每個模組都有明確職責。這樣的設計可以提升專案的可維護性，也方便未來擴充真實資料來源或加入排程工具。

環境建置方面，我使用 Docker Compose 建立 PostgreSQL 本機環境，讓資料庫可以快速啟動並保持環境一致性。Load 階段則使用 transaction 機制，確保資料寫入具備一致性；如果寫入過程發生錯誤，可以 rollback 回寫入前的狀態，避免資料只寫入一半。

此外，資料表使用 `stock_id` 與 `trade_date` 作為 unique key，避免同一檔股票在同一個交易日被重複寫入。Validation 層則負責在資料進入 Load 前進行資料品質檢查，例如檢查負數價格、負數交易量、`high_price` 小於 `low_price`，以及關鍵欄位缺失等問題。

今天學到的重點是：一個 ETL Pipeline 不只是要能成功執行，還要考慮資料品質、重複執行、交易一致性、模組化設計與環境可重現性。透過第一週的 MVP，我完成了一個可以執行、可以測試、也可以解釋設計思路的資料工程作品基礎版本。
