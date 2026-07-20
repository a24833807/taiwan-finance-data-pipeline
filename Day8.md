## Day 8 學習紀錄：TWSE API Spike

今天開始將資料來源從原本的 CSV，逐步升級為真實公開金融資料來源。

本日重點不是直接修改正式 ETL Pipeline，而是先建立一個獨立的 spike script，測試 TWSE 公開資料是否可以正常取得，並觀察回傳資料的結構。

今天完成的內容如下：

1. 使用 `requests` 呼叫 TWSE `MI_INDEX` endpoint，測試是否能取得每日收盤行情資料。
2. 使用 `trade_date = "20240701"` 作為指定查詢日期。
3. 觀察 API 回傳的 JSON 結構，包括 response keys、stat、不同資料區塊的筆數與第一筆資料。
4. 發現 TWSE 回傳結構不一定包含 `fields9` 這個 key，資料可能會放在新版的 `tables` 結構中。
5. 從符合每日股價資料的表格中取得欄位名稱與資料內容，建立 pandas DataFrame。
6. 印出 DataFrame 的 shape、columns 和前幾筆資料，確認資料可以被 Python 後續處理。
7. 調整測試腳本，避免硬寫單一 key，讓程式可以同時支援舊格式與新版表格格式。

透過今天的練習，我理解到真實資料來源接入前，不應該一開始就直接整合進正式 ETL，而是要先用獨立測試腳本確認 API 是否可用、回傳格式是否穩定、資料區塊是否符合需求。也學到公開 API 的回傳格式可能會變動，因此程式應該先觀察結構，再用較穩健的方式找出需要的資料，最後再決定如何接進既有的 `extract → transform → validate → load` 流程。
