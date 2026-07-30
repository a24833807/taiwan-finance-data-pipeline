# Day 16：Airflow 本機環境與最小 DAG

## 一、今日學習目標

今天開始將 Apache Airflow 導入 `taiwan-finance-data-pipeline` 專案。

今日目標是建立最小可執行的 Airflow 環境，並完成以下流程：

```text
啟動 Airflow
→ 開啟 Airflow Web UI
→ 載入 hello_airflow DAG
→ 手動觸發 DAG
→ 執行 print_hello Task
→ 查看 Task 執行狀態與 Log
```

今天暫時不串接既有 TWSE ETL，先確認 Airflow 本身可以正常運作。

---

## 二、Airflow 在專案中的角色

目前專案原本需要手動執行：

```bash
python src/main.py --trade-date 20240701
```

現有 Python 程式負責：

```text
Extract
→ Transform
→ Validate
→ Load
```

Airflow 將負責：

```text
排程
工作流程協調
Task 執行順序
失敗重試
執行狀態管理
日誌查看
```

因此 Airflow 不會取代現有 ETL 程式。

正確分工是：

```text
Python ETL
→ 負責資料處理邏輯

Airflow
→ 負責何時執行、如何協調、失敗如何處理
```

---

## 三、今日使用 Codex 的方式

今天不是直接複製固定的 Docker Compose 檔案，而是將任務需求交給 Codex，讓 Codex先檢查目前專案狀態。

Codex 在修改前需要先確認：

```text
專案目前目錄結構
既有 docker-compose.yml
既有 PostgreSQL Service 名稱
既有 PostgreSQL Port
既有 Docker Volume
.env 與 .env.example
.gitignore
是否已經存在 Airflow 相關檔案
```

這樣做的原因是避免新建立的 Airflow 環境與既有專案發生衝突。

可能的衝突包括：

```text
Service 名稱重複
Container 名稱重複
Host Port 重複
Volume 名稱重複
Network 設定衝突
環境變數互相覆蓋
```

重要理解：

```text
使用 Codex 修改專案前，
仍需要先讓它理解現有專案結構，
不能直接要求它套用通用範例。
```

---

## 四、今日預計新增的專案結構

Airflow 相關檔案使用獨立目錄管理：

```text
taiwan-finance-data-pipeline/
│
├── airflow/
│   ├── config/
│   ├── dags/
│   │   └── hello_airflow_dag.py
│   ├── logs/
│   └── plugins/
│
├── docker-compose.airflow.yml
├── docker-compose.yml
├── src/
├── tests/
├── sql/
└── README.md
```

使用獨立的：

```text
docker-compose.airflow.yml
```

而不是直接大幅修改現有的：

```text
docker-compose.yml
```

目的是將兩種環境分開：

```text
原本 docker-compose.yml
→ 專案 PostgreSQL 與既有開發環境

docker-compose.airflow.yml
→ Airflow 學習與排程環境
```

---

## 五、Airflow 目錄用途

### airflow/dags

用來存放 Airflow DAG 定義檔。

例如：

```text
airflow/dags/hello_airflow_dag.py
```

Airflow 的 DAG Processor 會定期讀取此目錄，解析有哪些 DAG、Task 與依賴關係。

---

### airflow/logs

用來存放 Airflow 執行過程所產生的 Log。

Log 可能包含：

```text
Task 開始時間
Task 結束時間
Python print 輸出
錯誤訊息
Retry 紀錄
執行環境資訊
```

Log 屬於執行產物，不應全部提交至 Git。

因此 `.gitignore` 應排除：

```gitignore
airflow/logs/*
```

如需保留空目錄，可以加入：

```gitignore
airflow/logs/*
!airflow/logs/.gitkeep
```

---

### airflow/plugins

用來放置自訂 Airflow Plugin。

Day 14 尚未使用自訂 Plugin，但先建立目錄，符合 Airflow 掛載結構。

---

### airflow/config

用來放置 Airflow 額外設定檔。

目前只先建立目錄，暫時不加入複雜設定。

---

## 六、第一個最小 DAG

新增檔案：

```text
airflow/dags/hello_airflow_dag.py
```

內容：

```python
from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    @task
    def print_hello() -> None:
        print("Hello Airflow")
        print("Taiwan Finance Data Pipeline")

    print_hello()
```

---

## 七、最小 DAG 程式說明

### dag_id

```python
dag_id="hello_airflow"
```

`dag_id` 是 DAG 在 Airflow 中的唯一識別名稱。

Airflow Web UI 中會顯示：

```text
hello_airflow
```

---

### start_date

```python
start_date=datetime(2026, 7, 1)
```

`start_date` 代表這個 DAG 從哪個時間點開始具有排程資格。

它不代表 Airflow 一啟動就一定會從該日期開始執行。

實際是否建立 DAG Run，還會受到以下設定影響：

```text
schedule
catchup
目前時間
資料區間
```

---

### schedule=None

```python
schedule=None
```

代表這個 DAG 沒有自動排程。

目前只能透過 Airflow Web UI 或命令手動觸發。

今天先使用手動執行，是為了避免同時處理：

```text
排程頻率
Logical Date
Data Interval
Catchup
歷史補跑
```

---

### catchup=False

```python
catchup=False
```

代表 Airflow 不會根據過去的 `start_date` 自動建立多個歷史 DAG Run。

Day 14 先關閉 Catchup，讓測試範圍保持單純。

---

### @task

```python
@task
def print_hello() -> None:
```

`@task` 會將一般 Python 函式包裝成 Airflow Task。

函式中的程式會在 Task 被執行時運作。

---

### 建立 Task

```python
print_hello()
```

這行會實際在 DAG 中建立 `print_hello` Task。

只有宣告函式但沒有呼叫時，Task 不會被加入 DAG。

---

## 八、目前 DAG 結構

目前只有一個 DAG 與一個 Task：

```text
hello_airflow DAG
│
└── print_hello Task
```

這個 DAG 不會：

```text
呼叫 TWSE API
執行 Transform
執行 Validate
寫入 PostgreSQL
呼叫 run_pipeline()
```

它目前只用來驗證 Airflow 是否能正常載入及執行 Python Task。

---

## 九、Airflow Docker Compose 架構

Airflow 本機環境預計包含：

```text
Airflow API Server / Web UI
Airflow Scheduler
Airflow DAG Processor
Airflow Worker
Airflow Init
Redis
Airflow Metadata PostgreSQL
```

不同元件有不同責任。

### API Server / Web UI

提供 Airflow 操作介面，可以查看：

```text
DAG 清單
DAG Run 狀態
Task 狀態
Task Log
排程資訊
失敗與重試紀錄
```

預計透過以下網址開啟：

```text
http://localhost:8080
```

---

### Scheduler

Scheduler 會檢查 DAG 排程，並判斷哪些 Task 已符合執行條件。

主要流程：

```text
讀取 DAG
→ 判斷是否需要建立 DAG Run
→ 判斷哪些 Task 可以執行
→ 將 Task 排入執行
```

---

### DAG Processor

DAG Processor 會讀取 `airflow/dags` 中的 Python 檔案，解析：

```text
DAG 是否有效
有哪些 Task
Task 依賴關係
排程設定
```

如果 DAG Python 檔有語法或 Import 錯誤，DAG 可能不會出現在 Web UI。

---

### Worker

Worker 負責真正執行 Task。

例如：

```text
Scheduler
→ 判斷 print_hello 可以執行

Worker
→ 實際執行 print_hello 函式
```

---

### Redis

Redis 在此架構中主要作為 Task 訊息傳遞的中介服務。

Scheduler 將工作安排好後，Worker 可以透過訊息佇列取得需要執行的工作。

---

### Airflow Init

Airflow Init 負責第一次環境初始化，例如：

```text
初始化 Metadata Database
建立必要資料表
建立預設使用者
執行資料庫 Migration
```

它不是持續運作的主要服務，通常初始化完成後就會結束。

---

## 十、Airflow Metadata Database

Airflow 需要一個自己的 PostgreSQL，儲存 Airflow 相關資訊。

例如：

```text
DAG Run 狀態
Task Instance 狀態
執行時間
成功或失敗紀錄
使用者資料
Connections
Variables
```

要特別區分：

```text
Airflow Metadata PostgreSQL
→ 儲存 Airflow 系統與執行狀態

專案 PostgreSQL
→ 儲存 stock_daily_price 股價資料
```

這兩個資料庫用途不同，不應混在一起。

---

## 十一、環境變數管理

Airflow 所需帳號、密碼與 UID 等設定，不應直接散落在多個檔案中。

可以建立：

```text
.env.airflow
```

並提供範例檔：

```text
.env.airflow.example
```

真正的 `.env.airflow` 不應提交到 Git，因此要加入：

```gitignore
.env.airflow
```

重要理解：

```text
Compose 檔負責描述服務架構，
環境變數檔負責提供不同環境的設定值。
```

---

## 十二、Airflow 操作指令

以下指令需在專案根目錄執行。

### 初始化 Airflow

```powershell
docker compose -f docker-compose.airflow.yml up airflow-init
```

用途：

```text
啟動 Metadata PostgreSQL
執行 Airflow Database 初始化
建立 Airflow 使用者
完成必要設定
```

---

### 啟動 Airflow

```powershell
docker compose -f docker-compose.airflow.yml up -d
```

`-d` 代表以背景模式啟動。

---

### 查看 Container 狀態

```powershell
docker compose -f docker-compose.airflow.yml ps
```

用來確認各個 Airflow Service 是否正常運作。

---

### 查看 Log

```powershell
docker compose -f docker-compose.airflow.yml logs
```

也可以查看特定服務：

```powershell
docker compose -f docker-compose.airflow.yml logs airflow-scheduler
```

---

### 停止 Airflow

```powershell
docker compose -f docker-compose.airflow.yml down
```

停止並移除 Container，但通常會保留 Volume 中的資料。

---

### 停止並清除 Volume

```powershell
docker compose -f docker-compose.airflow.yml down -v
```

此指令會一併刪除 Airflow Metadata Database 等 Volume 資料。

執行後下次可能需要重新初始化 Airflow。

---

## 十三、DAG 驗證流程

Airflow 環境啟動後，需要完成以下驗證：

```text
1. 開啟 http://localhost:8080
2. 登入 Airflow
3. 找到 hello_airflow
4. 啟用或手動 Trigger DAG
5. 等待 DAG Run 完成
6. 查看 print_hello Task
7. 開啟 Task Log
```

成功時應看到：

```text
DAG Run：success
Task：success
```

Task Log 應包含：

```text
Hello Airflow
Taiwan Finance Data Pipeline
```

---

## 十四、問題排查方向

### Web UI 無法開啟

可檢查：

```text
Docker Desktop 是否正常執行
Port 8080 是否被其他程式使用
API Server 是否啟動
Container 是否一直重新啟動
```

指令：

```powershell
docker compose -f docker-compose.airflow.yml ps
docker compose -f docker-compose.airflow.yml logs
```

---

### UI 看不到 hello_airflow

可檢查：

```text
hello_airflow_dag.py 是否放在 airflow/dags
DAG Volume 是否正確掛載
DAG 檔案是否有 Python 語法錯誤
Airflow SDK Import 是否符合目前 Airflow 版本
DAG Processor 是否正常
```

---

### DAG 執行失敗

可檢查：

```text
print_hello Task Log
Worker Log
Scheduler Log
DAG Import Error
```

需要先閱讀實際錯誤訊息，而不是直接重新安裝整套環境。

---

## 十五、今日沒有修改的內容

Day 14 僅建立 Airflow 最小學習環境。

今天不修改：

```text
src/main.py
run_pipeline()
extract.py
transform.py
validate.py
load.py
既有 Unit Test
既有 Integration Test
TWSE API 邏輯
股價資料庫 Schema
```

也不會把現有 ETL 拆成多個 Airflow Task。

---

## 十六、今日重要觀念

### Airflow 與 ETL 的責任分離

```text
ETL 程式負責資料處理。
Airflow 負責排程與工作流程管理。
```

### DAG 與 Task

```text
DAG 描述完整工作流程。
Task 是流程中的單一執行單位。
```

### Operator / @task

```text
Operator 或 @task 是建立 Task 的方式。
```

### Metadata Database

```text
Metadata Database 儲存 Airflow 執行狀態，
不儲存 TWSE 股價資料。
```

### 最小化導入

```text
先確認 Airflow 可以執行一個最小 Task，
再串接既有 run_pipeline()。
```

### Codex 使用原則

```text
修改前先檢查專案。
先列出修改計畫。
避免覆蓋既有架構。
不要一次加入過多功能。
```

---

## 十七、今日實作結果

### Codex 修改前檢查

```text
既有 PostgreSQL Service：
既有 PostgreSQL Port：
既有 Docker Volume：
發現的可能衝突：
```

### 新增或修改檔案

```text
[ ] airflow/dags/hello_airflow_dag.py
[ ] airflow/logs/.gitkeep
[ ] airflow/plugins/.gitkeep
[ ] airflow/config/.gitkeep
[ ] docker-compose.airflow.yml
[ ] .env.airflow.example
[ ] .gitignore
```

### Airflow 執行結果

```text
Airflow Init 是否成功：
Airflow Web UI 是否可以開啟：
hello_airflow 是否出現在 UI：
DAG Run 狀態：
print_hello Task 狀態：
```

### Task Log

```text
實際 Log 內容：
```

---

## 十八、Day 14 完成標準

完成以下項目後，才算 Day 14 實作完成：

```text
[ ] Airflow 相關目錄建立完成
[ ] Docker Compose Airflow 環境建立完成
[ ] Airflow 初始化成功
[ ] Airflow 服務正常啟動
[ ] 可以開啟 Web UI
[ ] hello_airflow DAG 成功載入
[ ] 可以手動觸發 DAG
[ ] print_hello Task 執行成功
[ ] Task Log 顯示正確文字
[ ] 沒有修改既有 ETL 邏輯
```

---

## 十九、下一步

完成 Airflow 最小 DAG 後，下一階段才會將現有：

```python
run_pipeline(trade_date)
```

接入 Airflow Task。

預計架構：

```text
Airflow DAG
│
└── run_twse_pipeline Task
        │
        └── run_pipeline(trade_date)
                │
                ├── Extract
                ├── Transform
                ├── Validate
                └── Load
```

後續還會學習：

```text
Logical Date
Data Interval
每日排程
Retry
Task 依賴
Catchup
Airflow Backfill
```
