# Day 3 學習日誌：將資料來源從 Mock Data 改為 CSV

今天主要完成 `taiwan-finance-data-pipeline` 專案的資料來源調整，將原本寫在程式內部的 mock dict data，改成從外部 CSV 檔案讀取資料。

原本的資料來源是直接寫在 `extract.py` 裡的 dict，因此資料內容與程式邏輯綁在一起。如果要調整測試資料，就需要直接修改 Python 程式碼。今天我將資料移到 `data/raw/stock_daily_price_sample.csv`，並修改 `extract.py`，讓 ETL 流程可以從 CSV 檔案讀取結構化資料。

這樣的調整讓資料來源更有彈性，也讓 Extract 層更接近實務上的資料工程流程。在實際專案中，原始資料通常不會直接寫死在程式碼裡，而是來自 CSV、API、Database、SFTP 或其他外部資料來源。
