# Day 22｜Airflow Variables / Connections / Secrets

## 今日目標

學習如何將 Pipeline 的設定值與外部系統連線資訊交由 Airflow 管理，而不是 Hard Code 在 DAG 程式中。

核心概念：

```text
Code
≠
Configuration
≠
Connection / Credentials
```

---

## 1. 為什麼不要 Hard Code？

如果直接在 DAG 裡寫：

```python
environment = "dev"

DB_HOST = "postgres"
DB_PORT = 5432
DB_USER = "postgres"
DB_PASSWORD = "password"
```

會有幾個問題：

1. Dev / Test / Prod 環境切換時需要修改程式。
2. DB Password 等敏感資訊可能被 Commit 到 Git。
3. 設定與程式邏輯混在一起，不容易維護。
4. Credential 可能意外出現在 GitHub。

因此應將：

```text
一般設定
→ Airflow Variable

外部系統連線設定
→ Airflow Connection
```

---

# 2. Airflow Variable

Variable 是 Airflow 管理的 Key-Value 設定。

例如：

```text
Key:
pipeline_environment

Value:
dev
```

用途：

```text
environment
table_name
bucket_name
feature flag
一般 Pipeline 設定
```

程式可以透過 Airflow 取得：

```python
from airflow.sdk import Variable

environment = Variable.get("pipeline_environment")
```

流程：

```text
Airflow Variable

pipeline_environment = dev
          ↓
         DAG
          ↓
Variable.get()
          ↓
        "dev"
```

### 核心理解

```text
Variable
→ 管理一般 Configuration
```

而不是把：

```python
environment = "dev"
```

Hard Code 在 DAG 裡。

---

# 3. Airflow Connection

Connection 用來管理外部系統的連線資訊。

例如 PostgreSQL：

```text
Connection ID:
taiwan_finance_postgres

Connection Type:
Postgres

Host:
postgres

Port:
5432

Database:
finance_db

Login:
postgres

Password:
********
```

程式可以透過 Connection ID 取得設定：

```python
from airflow.sdk.bases.hook import BaseHook

connection = BaseHook.get_connection(
    "taiwan_finance_postgres"
)
```

例如取得：

```python
connection.host
connection.port
connection.schema
```

### 核心理解

```text
Connection
→ 管理外部系統連線資訊
```

例如：

```text
DB Host
DB Port
Database
Username
Password
```

---

# 4. Variable vs Connection

| 類型                 | Variable | Connection |
| -------------------- | -------- | ---------- |
| Pipeline Environment | ✓        |            |
| Table Name           | ✓        |            |
| Bucket Name          | ✓        |            |
| Feature Flag         | ✓        |            |
| DB Host              |          | ✓          |
| DB Port              |          | ✓          |
| Database             |          | ✓          |
| Username             |          | ✓          |
| Password             |          | ✓          |

簡單判斷：

```text
一般設定值
→ Variable

要連接某個外部系統
→ Connection
```

---

# 5. Secrets

Password、API Key、Token 等屬於敏感資訊。

例如：

```text
DB Password
API Key
Access Token
Secret Key
```

這些資訊：

```text
不應 Hard Code 在 DAG
不應 Commit 到 Git
不應直接輸出到 Log
```

例如不要：

```python
print(connection.password)
```

因為可能造成：

```text
Password
   ↓
Airflow Task Log
   ↓
敏感資訊洩漏
```

---

# 6. Secrets Backend

更成熟的 Production 環境可能使用：

```text
Airflow
   ↓
Secrets Backend
   ↓
AWS Secrets Manager
```

Secrets Backend 可以讓 Credential 交由專門的秘密管理服務處理。

目前專案不需要立即導入。

現階段只需要理解：

```text
Variable
→ 一般設定

Connection
→ 外部系統連線資訊 / Credential

Secrets Backend
→ 更進階的秘密管理
```

---

# 7. 本日 Demo

新增：

```text
airflow/dags/variable_connection_demo_dag.py
```

DAG：

```text
variable_connection_demo
```

Task Dependency：

```text
show_variable
      ↓
show_connection
```

---

## show_variable

從 Airflow Variable 取得：

```text
pipeline_environment
```

程式：

```python
environment = Variable.get(
    "pipeline_environment"
)

print(
    f"Pipeline environment: {environment}"
)
```

實際執行：

```text
Pipeline environment: dev
```

代表：

```text
Airflow Variable
        ↓
DAG
        ↓
成功取得設定
```

---

## show_connection

從 Airflow Connection 取得：

```text
taiwan_finance_postgres
```

程式：

```python
connection = BaseHook.get_connection(
    "taiwan_finance_postgres"
)

print(f"Connection host: {connection.host}")
print(f"Connection port: {connection.port}")
print(f"Connection database: {connection.schema}")
```

實際執行：

```text
Connection host: postgres
Connection port: 5432
Connection database: finance_db
```

代表：

```text
Airflow Connection
        ↓
DAG
        ↓
成功取得 PostgreSQL Connection
```

---

# 8. Docker Container Connection

Airflow 與 PostgreSQL 都在 Docker Container 中。

Container-to-Container：

```text
Airflow Worker
      ↓
postgres:5432
      ↓
PostgreSQL Container
```

因此 Host 使用：

```text
postgres
```

而不是：

```text
localhost
```

原因：

```text
Container 裡面的 localhost
=
目前這個 Container 自己
```

Service Name 才能用來找到另一個 Container。

---

# 9. Deprecated Warning

實作過程遇到：

```text
airflow.hooks.base.BaseHook
is deprecated
```

以及：

```text
Variable.get from airflow.models
is deprecated
```

`deprecated` 代表：

```text
現在
→ 還可以使用

但是
→ 官方已不推薦

未來
→ 可能移除
```

因此新版 Airflow 改用：

```python
from airflow.sdk import Variable
from airflow.sdk.bases.hook import BaseHook
```

### Warning 與 Error

```text
ERROR
→ 執行發生問題
→ 通常需要處理

WARNING
→ 程式可能仍正常執行
→ 需要閱讀內容判斷

DeprecationWarning
→ 舊 API 目前仍能使用
→ 但應逐步改成新版 API
```

---

# 10. Secret Masking Warning

實作時也看到：

```text
Skipping masking for a secret
as it's too short (<5 chars)
```

Airflow 有 Secret Masking 機制：

```text
Secret
↓
Airflow Log
↓
***
```

避免 Credential 直接出現在 Log。

如果某個 Secret 太短，例如：

```text
dev
```

只有 3 個字元，Airflow 可能不進行 Masking，以避免大量正常文字被錯誤遮蔽。

本次：

```text
pipeline_environment = dev
```

本身不是敏感資訊，因此不影響此次學習結果。

---

# 11. Day 21 架構理解

以前：

```text
DAG
│
├── environment = "dev"
├── DB_HOST = "postgres"
├── DB_PORT = 5432
├── DB_USER = "postgres"
└── DB_PASSWORD = "password"
```

現在：

```text
                     ┌→ Airflow Variable
                     │      ↓
                     │   一般設定
DAG ─────────────────┤
                     │
                     └→ Airflow Connection
                            ↓
                       外部系統連線資訊
```

未來更成熟的架構可能：

```text
DAG
 ↓
Airflow Connection
 ↓
Secrets Backend
 ↓
AWS Secrets Manager
```

---

# 12. 今天學會

1. Airflow Variable 可以管理 Pipeline 的一般設定值，避免將設定 Hard Code 在 DAG 中。

2. Airflow Connection 可以集中管理 Database 等外部系統的連線資訊，例如 Host、Port、Database、Username、Password。

3. Variable 與 Connection 用途不同：

```text
Variable
→ 一般設定

Connection
→ 外部系統連線資訊
```

4. Password、API Key 等敏感資訊不應 Hard Code，也不應直接輸出到 Airflow Log。

5. Secrets Backend 是更進階的秘密管理機制，目前先理解概念，不需要立即導入。

6. Deprecated Warning 不等於執行失敗，而是代表目前使用的 API 已不推薦，未來可能被移除。

---

# Day 21 核心整理

```text
DAG
│
├── Airflow Variable
│      ↓
│   一般 Configuration
│
├── Airflow Connection
│      ↓
│   外部系統連線資訊
│
└── Secrets Backend
       ↓
    更進階的秘密管理
```

核心原則：

```text
不要把所有設定都 Hard Code 在程式中。

一般設定
→ Variable

外部系統連線
→ Connection

敏感資訊
→ 不進 Git、不印 Log
```

**Day 22：Airflow Variables / Connections / Secrets 完成。**
