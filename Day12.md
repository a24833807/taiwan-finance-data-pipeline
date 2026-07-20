# Day 12 學習筆記：將本機專案上傳至 GitHub

今天將原本只存在本機的 `taiwan-finance-data-pipeline` 專案，建立 Git 版本控制並推送到 GitHub Repository。

## 1. 確認 `.env` 已被 `.gitignore` 排除

在上傳專案前，先確認 `.env` 已加入 `.gitignore`，避免本機環境設定被納入 Git 版本控制。

`.env` 可能包含：

- PostgreSQL 使用者名稱
- PostgreSQL 密碼
- 資料庫名稱
- API Key
- 其他環境相關設定

這類資訊不應該提交到 GitHub。

專案只保留不含真實機敏資料的：

```text
.env.example
```

讓其他使用者知道執行專案時需要設定哪些環境變數。

## 2. 確認沒有上傳機敏資料

在執行 `git add` 與 `git commit` 前，先檢查準備提交的檔案，確認沒有包含：

```text
.env
API Key
資料庫帳號
資料庫密碼
其他個人或系統機敏資訊
```

這次練習讓我理解到，將程式上傳到公開 Repository 前，必須先進行敏感資訊檢查，而不是直接執行 `git add .` 後推送。

## 3. 完成本機 Git 初始化

在專案根目錄執行 Git 初始化，讓目前的專案資料夾成為 Git Repository。

```bash
git init
```

初始化後，Git 可以追蹤專案檔案的新增、修改與刪除狀態。

## 4. 建立第一次 Commit

將專案檔案加入暫存區，並建立第一個版本紀錄：

```bash
git add .
git commit -m "Initial commit"
```

第一次 commit 代表目前專案的一個完整版本快照。

這讓後續每次修改都可以和這個版本比較，也可以查看專案的修改歷史。

## 5. 在 GitHub 建立空 Repository

在 GitHub 建立：

```text
taiwan-finance-data-pipeline
```

Repository 建立時先保持空白，不額外建立 README、`.gitignore` 或 License，避免與本機既有檔案產生衝突。

GitHub Repository 是本機 Git Repository 對應的遠端儲存位置。

## 6. 設定 Origin Remote

使用 `git remote add`，將本機 Repository 與 GitHub Repository 連接：

```bash
git remote add origin <GitHub Repository URL>
```

其中：

```text
origin
```

是遠端 Repository 的慣用名稱。

可以透過以下指令確認設定：

```bash
git remote -v
```

## 7. 將 Main 分支推送到 GitHub

將本機主要分支名稱設定為 `main`，並推送到 GitHub：

```bash
git branch -M main
git push -u origin main
```

`-u` 會建立本機 `main` 與遠端 `origin/main` 之間的追蹤關係。

完成設定後，後續通常只需要執行：

```bash
git push
```

即可推送新的 commit。

## 8. 確認 GitHub 專案內容

推送完成後，在 GitHub 上確認可以看到完整的專案內容，例如：

```text
README.md
requirements.txt
docker-compose.yml
src/
tests/
scripts/
sql/
.env.example
.gitignore
```

同時確認 GitHub 上沒有出現：

```text
.env
API Key
資料庫密碼
其他機敏資料
```

這代表 `.gitignore` 與上傳前的檢查都有正確生效。

## 9. 完成第二次 Commit 與 Push

在第一次上傳後，再修改專案內容並完成第二次版本提交：

```bash
git add .
git commit -m "Update project documentation"
git push
```

透過第二次操作，完整練習了日常開發流程：

```text
修改程式
→ 檢查 Git 狀態
→ 加入暫存區
→ 建立 Commit
→ Push 到 GitHub
```

## 今日理解

今天理解了 Git 與 GitHub 的差異：

```text
Git
→ 本機版本控制工具

GitHub
→ 儲存與分享 Git Repository 的遠端平台
```

也理解了 Commit 與 Push 的差異：

```text
Commit
→ 將程式版本記錄在本機 Repository

Push
→ 將本機 Commit 上傳到遠端 Repository
```

目前專案已經不只存在個人電腦中，而是具備：

- 本機版本控制
- 遠端程式碼備份
- Commit 修改歷史
- 公開作品集展示
- 後續 CI/CD 整合基礎

這也為下一步使用 GitHub Actions 自動執行 `pytest` 做好準備。
