# Day 5 學習日誌：Load 層交易控制與資料入庫可靠性

今天主要強化 `taiwan-finance-data-pipeline` 專案中的 `load.py`，目標是讓資料入庫流程更穩定，並更接近實務資料工程中的 ETL Load 設計。

今天完成的第一個重點是處理空資料情境。當來源資料或 Transform 後的資料為空時，程式會記錄 log，並回傳 `0`，表示本次沒有資料需要寫入。這樣可以避免 Pipeline 在沒有資料時仍然嘗試執行資料庫寫入，降低不必要的錯誤風險。

第二個重點是加入 transaction 控制。這次使用 SQLAlchemy 的 `engine.begin()` 來執行資料寫入。透過 transaction，可以確保資料寫入具備一致性：如果整批資料成功寫入，就會 commit；如果過程中發生錯誤，則會 rollback 回寫入前的狀態，避免資料只寫入一半，造成資料庫狀態不一致。

第三個重點是保留重複資料處理機制。Load 階段仍然使用 `stock_id` 與 `trade_date` 作為 duplicate handling 的依據，確保同一檔股票在同一個交易日期不會被重複寫入。這讓 ETL Job 可以重複執行，但不會產生重複資料。

第四個重點是回傳實際寫入資料庫的筆數。Load function 最後會回傳本次實際 loaded rows，而不是單純回傳 Transform 後的資料筆數。這樣可以更準確地知道資料庫實際新增了幾筆資料。例如，當同一批資料第二次執行時，Transform rows 可能仍然是 3 筆，但 Loaded rows 會是 0，代表資料已存在，沒有重複寫入。

最後，我也保留了 `logger.info`，用來記錄實際寫入資料庫的筆數。這可以提升 ETL Pipeline 的可觀測性，方便未來排查批次執行狀況。

今天學到的重點是：Load 層不只是把資料 insert 到資料庫，而是需要考慮空資料、transaction、rollback、重複資料處理，以及實際寫入筆數的紀錄。這些設計可以讓 ETL Pipeline 更穩定，也更符合實務資料工程中對資料一致性與批次可重跑性的要求。
