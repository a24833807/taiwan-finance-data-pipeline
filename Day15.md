# Day 15 學習筆記：將 PostgreSQL Integration Test 接入 CI

今天將 PostgreSQL Integration Test 整合進 GitHub Actions，讓 CI 不只驗證單一 Python 函式，也能測試 Load 模組與 PostgreSQL 之間的實際整合。

目前 CI 流程為：

```text
Push 或 Pull Request
→ 建立 GitHub Actions 執行環境
→ 啟動 PostgreSQL Service Container
→ 初始化測試資料表
→ 執行 Unit Test
→ 執行 Integration Test
→ 回報測試結果
```

## 1. 建立 PostgreSQL Service Container

在 GitHub Actions Workflow 中建立 PostgreSQL Service Container，作為 Integration Test 執行期間使用的暫時性資料庫服務。

Service Container 的生命週期只存在於單次 Workflow 執行期間：

```text
Workflow 開始
→ 建立 PostgreSQL Container
→ 執行資料庫測試
→ Workflow 結束
→ Container 與測試資料一併清除
```

因此，CI 不需要連線到開發或正式資料庫，也不會留下測試資料。

透過 Service Container，可以在 GitHub Actions 的全新環境中測試：

- PostgreSQL 連線
- Database schema
- SQLAlchemy
- Load transaction
- Duplicate handling
- 實際資料寫入與查詢

## 2. CI 環境可以重新建立

CI 每次執行時，都會從乾淨環境重新建立測試所需的元件，包括：

```text
專案程式碼
Python 執行環境
requirements.txt 中的依賴套件
PostgreSQL 測試資料庫
資料表結構
測試資料
```

這可以確認專案不會只在本機既有環境中正常運作。

若缺少套件、資料表沒有初始化，或資料庫設定不完整，GitHub Actions 就會在重新建立環境時發現問題。

## 3. 使用環境變數切換測試資料庫

Integration Test 不再只依賴寫死的本機資料庫 URL，而是優先從環境變數取得測試資料庫連線。

整體行為為：

```text
本機執行
→ 使用本機 PostgreSQL 測試資料庫

GitHub Actions 執行
→ 使用 Service Container 提供的 PostgreSQL
```

這讓同一套測試程式可以在不同環境執行，不需要為本機與 CI 各寫一套測試。

也代表資料庫設定與測試邏輯已經分離：

```text
測試程式
→ 定義要驗證的行為

環境變數
→ 提供目前環境的資料庫連線資訊
```

## 4. CI 開始具備多元件測試

原本的 CI 主要測試純 Python 邏輯，例如：

```text
日期驗證
日期區間產生
TWSE 欄位轉換
缺失值處理
```

加入 PostgreSQL Integration Test 後，CI 會同時驗證多個元件：

```text
pandas DataFrame
→ Load 模組
→ SQLAlchemy
→ psycopg2
→ PostgreSQL
→ Unique Constraint
→ 資料查詢與驗證
```

這代表測試範圍已從 Unit Test 擴大到 Integration Test。

Unit Test 可以確認單一函式是否符合預期；Integration Test 則可以確認多個元件組合後，是否能完成真實資料寫入流程。

## 今日理解

今天學習到 Service Container 可以在 CI 執行期間提供暫時性的外部服務，例如 PostgreSQL。

同時，也透過環境變數讓測試程式可以適應不同的執行環境：

```text
相同測試程式
＋
不同環境設定
→ 在本機與 GitHub Actions 中執行
```

目前 CI 已經能驗證：

- Python 環境是否可建立
- Dependencies 是否完整
- Unit Test 是否通過
- PostgreSQL 是否能啟動
- Database schema 是否能初始化
- Load 是否可以實際寫入資料
- 相同資料重跑是否會重複
- 資料庫查詢結果是否符合預期

這讓 CI 更接近真實 Data Pipeline 的執行環境，而不只是單純執行 Python 函式測試。
