# Taiwan Finance Data Pipeline

**目前狀態：** MVP 已完成。此專案目前可以從 CSV 原始資料讀取台股資料，透過 ETL 流程進行資料清洗、資料驗證，最後寫入 PostgreSQL，並具備重複資料處理、Transaction Control 與 Row Count Logging。

---

## 專案簡介

本專案是一個台灣金融資料 ETL Pipeline，目標是模擬實務上的資料工程流程。

此 Pipeline 會讀取台股每日股價資料，將原始資料清洗並轉換成標準化格式，接著進行資料品質檢查，最後將通過驗證的資料寫入 PostgreSQL 資料庫。

專案設計上包含資料擷取、資料清洗、資料驗證、資料表設計、重複資料處理、交易控制、執行紀錄，以及透過 Docker 建立可重現的本機開發環境。

目前 MVP 版本使用 CSV sample data 驗證 ETL 架構。未來版本會將 CSV sample data 替換成真實公開金融資料來源，並加入 Airflow 排程。

---

## 專案目的

此專案的目的是練習使用現代資料工程的方式，建立一個可維護、可擴充的 Data Pipeline。

不同於將所有 ETL 邏輯寫在同一支 Python Script，本專案將流程拆分成多個模組：

- Extract：負責取得原始金融資料
- Transform：負責清洗與標準化資料
- Validate：負責資料品質檢查
- Load：負責將資料寫入 PostgreSQL
- Config：負責管理環境變數
- DB：負責管理資料庫連線

這樣的架構能讓專案更容易維護、測試、除錯與擴充。

---

## 使用技術

- Python
- pandas
- SQLAlchemy
- PostgreSQL
- Docker Compose
- python-dotenv
- Git

---

## 資料流程

```text
CSV Raw Data
        ↓
Extract
        ↓
Transform
        ↓
Validate
        ↓
Load
        ↓
PostgreSQL
```

目前 MVP 流程：

```text
data/raw/stock_daily_price_sample.csv
        ↓
extract.py
        ↓
transform.py
        ↓
validate.py
        ↓
load.py
        ↓
PostgreSQL: stock_daily_price table
```

目前此專案已將資料來源從程式內部的 mock data 改為外部 CSV 檔案，讓資料來源與程式邏輯解耦。Pipeline 會先從 `data/raw/stock_daily_price_sample.csv` 讀取原始資料，經過 Transform 層進行欄位標準化與型別轉換，再透過 Validate 層檢查資料品質，最後將通過驗證的資料寫入 PostgreSQL。

---

## 專案結構

```text
taiwan-finance-data-pipeline/
│
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .env.example
├── .gitignore
│
├── sql/
│   └── init.sql
│
├── data/
│   └── raw/
│       └── stock_daily_price_sample.csv
│
└── src/
    ├── main.py
    ├── extract.py
    ├── transform.py
    ├── validate.py
    ├── load.py
    ├── config.py
    └── db.py
```

---

## 模組職責

### `main.py`

ETL 流程的入口。

負責控制整個流程的執行順序：

```text
extract → transform → validate → load
```

同時也會記錄每個階段處理的資料筆數，包含 Extracted rows、Transformed rows、Validated rows 與 Loaded rows。

範例執行紀錄：

```text
Starting ETL pipeline
Extracted rows=3
Transformed rows=3
Validated rows=2
Loaded rows=2
ETL pipeline completed
```

---

### `extract.py`

負責取得原始股價資料。

目前 MVP 版本會從 `data/raw/stock_daily_price_sample.csv` 讀取 CSV 原始資料。未來版本會將此模組替換成真實公開金融資料來源，例如台灣證券交易所公開資料。

Extract 層只負責取得資料，不負責資料清洗與資料入庫。

---

### `transform.py`

負責清洗與標準化原始資料。

轉換後的資料會符合目標資料表 schema：

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

此層主要處理：

- 必要欄位檢查
- 欄位標準化
- 數字符號清除
- 價格欄位型別轉換
- 成交量型別轉換
- 日期格式處理

Transform 層的目標是將 raw data 轉換成符合後續 Validate 與 Load 階段使用的標準格式。

---

### `validate.py`

負責在資料寫入 PostgreSQL 前進行資料品質檢查。

目前驗證規則包含：

- DataFrame 不可為空
- 價格欄位不可為負數
- `volume` 不可為負數
- `high_price` 不可小於 `low_price`
- `stock_id` 不可為空
- `trade_date` 不可為空

若資料不符合驗證規則，該筆資料會被排除，並透過 warning log 記錄排除筆數。

Validate 層可以避免不合理資料進入 PostgreSQL，提升資料品質與 Pipeline 穩定性。

---

### `load.py`

負責將通過驗證後的資料寫入 PostgreSQL。

Load 流程會透過資料庫約束避免重複資料寫入，並回傳本次實際寫入的資料筆數。

此外，Load 階段使用 SQLAlchemy 的 `engine.begin()` 進行 transaction 控制。如果整批資料寫入成功，交易會 commit；如果寫入過程發生錯誤，交易會 rollback 回寫入前的狀態，避免資料只寫入一半造成資料庫狀態不一致。

這讓 Pipeline 更容易監控、除錯，也更接近實務資料工程中的資料入庫流程。

---

### `config.py`

負責讀取 `.env` 中的環境變數。

資料庫連線資訊不會寫死在程式碼中，方便未來在不同環境中切換設定。

---

### `db.py`

負責建立 SQLAlchemy database engine。

此設計將資料庫連線邏輯與 ETL 業務邏輯分離，提升程式可維護性。

---

## 資料庫設計

目標資料表為 `stock_daily_price`。

```sql
CREATE TABLE IF NOT EXISTS stock_daily_price (
    id SERIAL PRIMARY KEY,
    stock_id VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100),
    trade_date DATE NOT NULL,
    open_price NUMERIC(12, 2),
    high_price NUMERIC(12, 2),
    low_price NUMERIC(12, 2),
    close_price NUMERIC(12, 2),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (stock_id, trade_date)
);
```

其中 `stock_id` 與 `trade_date` 組成 unique constraint，用來避免同一檔股票在同一天的資料被重複寫入。

---

## Data Validation

在資料寫入 PostgreSQL 前，Pipeline 會先檢查 Transform 後的資料是否合理。

目前驗證規則包含：

- 價格欄位不可為負數
- 成交量不可為負數
- `high_price` 不可小於 `low_price`
- `stock_id` 不可為空
- `trade_date` 不可為空

如果資料不符合驗證規則，Validate 層會將該筆資料排除，並透過 warning log 記錄排除筆數。

這一層可以避免明顯不合理或缺少關鍵欄位的資料進入資料庫。

---

## 重複資料處理

此 Pipeline 設計成 idempotent，也就是同一個 ETL Job 可以重複執行，但不會產生重複資料。

例如：

第一次執行：

```text
Extracted rows=3
Transformed rows=3
Validated rows=3
Loaded rows=3
```

第二次執行相同資料：

```text
Extracted rows=3
Transformed rows=3
Validated rows=3
Loaded rows=0
```

這種設計在實務批次處理中很重要，因為 ETL Job 可能會因為失敗、補資料或資料驗證需求而重跑。

---

## Transaction Control

Load 階段使用 SQLAlchemy 的 `engine.begin()` 進行資料庫 transaction 控制。

交易控制的概念如下：

```text
開始寫入
    ↓
全部成功 → commit
    ↓
中途失敗 → rollback
```

這樣可以確保資料寫入的一致性。如果寫入過程中發生錯誤，整批資料會 rollback 回寫入前的狀態，避免資料庫出現部分寫入成功、部分寫入失敗的情況。

---

## Row Count Logging

Pipeline 會記錄每個階段的資料筆數：

- Extracted rows：從來源取得的資料筆數
- Transformed rows：完成清洗與型別轉換後的資料筆數
- Validated rows：通過資料品質檢查後的資料筆數
- Loaded rows：實際寫入 PostgreSQL 的資料筆數

這些 log 可以協助觀察資料在每個階段的變化，也方便未來排查批次執行異常。

---

## 為什麼拆成 Extract / Transform / Validate / Load？

本專案將 ETL 流程拆成不同模組，主要是為了提升可維護性與擴充性。

每個模組都有單一職責：

- `extract.py` 只負責取得資料
- `transform.py` 只負責清洗與標準化資料
- `validate.py` 只負責資料品質檢查
- `load.py` 只負責資料入庫

這樣的設計讓 Pipeline 更容易除錯與擴充。例如未來要將資料來源從 CSV sample data 換成真實 API，只需要主要調整 Extract 層，不需要重寫整個流程。

---

## 為什麼使用 PostgreSQL？

PostgreSQL 是一套開源、穩定且廣泛使用的關聯式資料庫，適合儲存結構化金融資料。

它支援 schema 設計、constraint、index 與 SQL 查詢分析。在本專案中，PostgreSQL 用來儲存清洗後的股價資料，並透過 unique constraint 避免重複資料寫入。

---

## 為什麼使用 Docker？

本專案使用 Docker Compose 建立可重現的本機開發環境。

透過 Docker，不需要在每台機器上手動安裝 PostgreSQL，只需要執行：

```bash
docker compose up -d
```

即可啟動資料庫環境。

這讓專案更容易設定、遷移與分享，也能確保不同開發環境之間的一致性。

---

## 如何執行專案

### 1. Clone repository

```bash
git clone <your-repository-url>
cd taiwan-finance-data-pipeline
```

### 2. 建立 `.env`

將 `.env.example` 複製成 `.env`。

```bash
cp .env.example .env
```

Windows PowerShell 可使用：

```powershell
Copy-Item .env.example .env
```

`.env` 範例：

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=finance_db
POSTGRES_USER=finance_user
POSTGRES_PASSWORD=finance_password
```

### 3. 使用 Docker Compose 啟動 PostgreSQL

```bash
docker compose up -d
```

### 4. 建立 Python 虛擬環境並安裝套件

```bash
python -m venv .venv
```

Windows：

```bash
.venv\Scripts\activate
```

安裝套件：

```bash
pip install -r requirements.txt
```

### 5. 執行 ETL Pipeline

```bash
python src/main.py
```

### 6. 驗證 PostgreSQL 資料

```bash
docker exec -it taiwan-finance-postgres psql -U finance_user -d finance_db
```

查詢資料筆數：

```sql
SELECT COUNT(*) FROM stock_daily_price;
```

查詢資料內容：

```sql
SELECT * FROM stock_daily_price;
```

---

## 測試情境

### 1. 正常資料寫入

當 CSV 資料格式正確且尚未寫入資料庫時，Pipeline 應可正常完成：

```text
Extracted rows=3
Transformed rows=3
Validated rows=3
Loaded rows=3
```

### 2. 重複執行

當同一批資料再次執行時，因為 `stock_id` 與 `trade_date` 已存在，資料不會重複寫入：

```text
Extracted rows=3
Transformed rows=3
Validated rows=3
Loaded rows=0
```

### 3. 錯誤資料排除

當資料中出現不合理紀錄，例如 `high_price` 小於 `low_price`，Validate 層會排除該筆資料：

```text
Extracted rows=3
Transformed rows=3
Validation removed 1 invalid rows
Validated rows=2
Loaded rows=2
```

---

## 目前完成進度

已完成：

- 建立 Python ETL 專案結構
- 建立 PostgreSQL table schema
- 使用 Docker Compose 啟動 PostgreSQL
- 將資料來源從 mock data 改為 CSV raw data
- 實作 Extract / Transform / Validate / Load 模組
- 加入 Transform 層資料清洗與型別轉換
- 加入 Validate 層資料品質檢查
- 使用 transaction 控制資料庫寫入
- 將股價資料寫入 PostgreSQL
- 加入 Row Count Logging
- 透過 unique constraint 處理重複資料

---

## Week 1 Summary

第一週完成了 `taiwan-finance-data-pipeline` 專案的 MVP 版本。

目前 Pipeline 已支援從 CSV 檔案讀取股價資料，將原始資料轉換成標準化 schema，進行資料品質驗證，並將通過驗證的資料寫入 PostgreSQL。

專案也包含 Docker Compose PostgreSQL 環境、`.env` 環境變數管理、transaction-based database loading、unique constraint duplicate handling，以及 row count logging。

透過這個 MVP，練習了現代資料工程的核心概念，包含模組化 ETL 設計、raw data handling、data cleaning、data validation、idempotent loading、transaction control，以及 reproducible local infrastructure。

---

## 未來改善方向

後續規劃：

1. 將 CSV sample data 替換成真實台股公開資料來源
2. 將每日原始資料保存至 `data/raw`
3. 加入 Airflow DAG 進行排程
4. 加入單元測試，驗證 Transform / Validate / Load 邏輯
5. 加入錯誤資料輸出機制，例如將 invalid records 存成 CSV 或寫入 error table
6. 加入 FastAPI endpoint 觸發 ETL Job
7. 部署至雲端環境
