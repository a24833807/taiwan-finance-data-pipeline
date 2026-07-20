# Day 2 學習日誌：PostgreSQL、Docker Compose 與 ETL 重複資料處理

今天主要完成 `taiwan-finance-data-pipeline` 專案的資料庫環境建置與驗證，重點放在 PostgreSQL、Docker Compose 設定，以及 ETL 重複執行時的資料防重機制。

今天主要完成三個項目：

1. **檢視 `docker-compose.yml` 設定**

   我確認 PostgreSQL 可以透過 Docker Compose 正常啟動，並將資料庫連線設定放在 `.env` 中，而不是直接 hardcode 在 `docker-compose.yml` 或程式碼裡。

   這樣的設計可以讓資料庫設定更有彈性，未來若需要更換環境、遷移系統或調整資料庫連線資訊，只需要修改 `.env`，不需要直接改動主要設定檔或程式碼。

2. **檢視 `sql/init.sql` 的資料表設計**

   我確認 `stock_daily_price` table 可以在 PostgreSQL 第一次初始化時自動建立。

   在資料表設計中，我設定了 `stock_id` 與 `trade_date` 的 unique constraint，用來避免同一檔股票在同一個交易日期被重複寫入。這樣可以降低重複資料進入資料庫的風險，避免後續分析或查詢時產生髒資料。

3. **測試 ETL 重複執行的情境**

   我總共執行兩次 ETL 程式。

   第一次執行時，資料可以正常寫入 PostgreSQL。第二次執行相同資料時，因為資料表已有 unique constraint，所以重複資料不會再次被寫入。

   透過這個測試，我理解到 ETL Pipeline 不只是要能把資料寫入資料庫，也需要考慮批次重跑的情境。在實務上，ETL Job 可能會因為失敗、資料修正或驗證需求而重複執行，因此需要設計成 idempotent，確保同一批資料重跑時不會產生重複資料。

今天學到的重點是：資料工程不只是完成資料搬移，也需要考慮環境設定、資料表約束、重複資料處理與批次重跑機制。這些設計可以讓 ETL Pipeline 更穩定，也更接近實務上的資料工程流程。
