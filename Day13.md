# Day 13 學習筆記：使用 GitHub Actions 建立自動測試流程

今天使用 GitHub Actions，為 `taiwan-finance-data-pipeline` 建立基礎的 CI 自動測試流程。

目前的自動化流程為：

```text
Push 或 Pull Request
→ 啟動 GitHub Actions
→ 建立 Ubuntu 執行環境
→ 安裝 Python
→ 安裝專案套件
→ 執行 pytest
→ 回報測試成功或失敗
```

## 1. 建立 GitHub Actions Workflow 設定檔

新增以下檔案：

```text
.github/workflows/test.yml
```

這個 YAML 檔案用來定義 GitHub Actions 的觸發條件、執行環境與測試步驟。

Workflow 設定檔也會跟著專案程式一起被 Git 版本控制，因此日後若要調整測試流程，也可以查看相關修改紀錄。

## 2. Push 與 Pull Request 時自動觸發

目前 Workflow 會在以下情況自動執行：

```text
Push 程式碼到 GitHub
建立或更新 Pull Request
```

這表示程式碼每次發生變更時，GitHub 都能自動驗證現有測試是否仍然通過，而不需要完全依賴開發者手動執行。

## 3. GitHub Actions 自動安裝 Python

GitHub Actions 會建立一個新的 Ubuntu 執行環境，並安裝 Workflow 指定的 Python 版本。

這次測試不是在本機 Windows 環境執行，而是在 GitHub 提供的乾淨環境重新建立專案。

因此，可以進一步確認專案不會只在自己的電腦上正常執行。

## 4. 根據 requirements.txt 安裝套件

GitHub Actions 會執行：

```bash
pip install -r requirements.txt
```

安裝專案需要的套件，例如：

```text
pandas
requests
SQLAlchemy
psycopg2-binary
python-dotenv
pytest
```

這項流程可以驗證 `requirements.txt` 是否完整。

如果程式需要某個套件，但沒有記錄在 `requirements.txt` 中，本機可能因為已經安裝而不會出錯，但 GitHub Actions 的全新環境會直接發現問題。

## 5. 自動執行 pytest

套件安裝完成後，Workflow 會執行：

```bash
pytest -v
```

`-v` 代表顯示較詳細的測試資訊，包括：

- 收集到哪些測試案例
- 每個測試案例的名稱
- 每個測試成功或失敗的狀態
- 測試失敗的位置與原因

只要有任何一項測試失敗，整個 Workflow 就會被標記為失敗。

## 6. 正常測試時 Workflow 通過

當所有測試案例都符合預期時，GitHub Actions 會顯示 Workflow 執行成功。

這代表：

```text
GitHub 成功取得程式碼
→ Python 環境建立成功
→ Dependencies 安裝成功
→ pytest 執行成功
→ 所有測試通過
```

GitHub 上通常會以綠色勾號表示這次 Commit 的測試結果正常。

## 7. 錯誤測試時 Workflow 失敗

為了確認 CI 確實能偵測問題，我暫時修改一個測試案例，讓實際結果與預期結果不一致。

Push 到 GitHub 後，GitHub Actions 正確顯示失敗，並指出發生錯誤的測試案例與 Assertion。

這證明 Workflow 不只是形式上的設定，而是真的會執行測試並攔截不符合預期的程式變更。

## 8. 修正後 Workflow 恢復成功

將錯誤的測試內容修正後，再次 Commit 並 Push。

GitHub Actions 重新執行完整流程，最終恢復為成功狀態。

完整驗證流程為：

```text
修改測試造成失敗
→ Commit
→ Push
→ GitHub Actions 顯示失敗
→ 修正測試
→ 再次 Commit 與 Push
→ GitHub Actions 恢復成功
```

## 今日理解

今天學習的重點是 CI，也就是 Continuous Integration（持續整合）。

原本的開發流程是：

```text
修改程式
→ 手動執行 pytest
→ Commit
→ Push
```

加入 GitHub Actions 後，流程變成：

```text
修改程式
→ Commit
→ Push
→ GitHub 自動執行 pytest
→ 回報測試結果
```

CI 的主要作用不是部署程式，而是在程式碼變更後，自動驗證專案能否建立，以及既有功能是否仍然正常。

這次練習也讓我理解到：

- 本機測試通過，不代表其他環境一定能通過。
- `requirements.txt` 是重新建立 Python 環境的重要依據。
- Workflow 失敗不是壞事，而是提前發現問題。
- 自動化測試能降低修改程式後忘記執行測試的風險。
- GitHub Actions Workflow 本身也是專案程式碼的一部分。

目前專案已具備：

```text
Git 版本控制
→ GitHub 遠端 Repository
→ pytest 單元測試
→ GitHub Actions 自動測試
```

這讓專案從單純的個人練習，進一步具備基本的軟體工程與持續整合流程。
