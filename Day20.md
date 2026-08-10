# Day 20：Airflow XCom 與 Task 間資料傳遞

## 今日完成內容

### 1. 使用 XCom 傳遞資料

Airflow 的 XCom（Cross Communication）可以讓不同 Task 之間交換小型資料。

例如：

```python
@task
def prepare() -> str:
    return "20260810"
```

`prepare` 的回傳值會由 Airflow 儲存成 XCom。

---

### 2. XCom 可以用於 Task 之間傳遞參數

下一個 Task 可以直接接收上一個 Task 的輸出：

```python
@task
def process(trade_date: str):
    print(trade_date)
```

建立 Task：

```python
trade_date = prepare()
process(trade_date)
```

實際執行概念：

```text
prepare Task
→ return "20260810"
→ Airflow 將結果存入 XCom
→ process Task 取得 XCom
→ trade_date = "20260810"
```

因此 Task 不需要在同一個 Python Process 或同一個 Worker 中共享記憶體。

---

## TaskFlow API 與 XCom

使用 `@task` 時，可以直接：

```python
result = prepare()
process(result)
```

Airflow 會自動處理底層的 XCom 傳遞。

因此目前不需要自己寫：

```python
xcom_push()
xcom_pull()
```

可以理解成：

```text
@task return value
→ 自動存入 XCom

下一個 @task 的參數
→ 自動取得上游 XCom
```

---

## XCom 適合傳什麼？

XCom 適合傳遞小型資訊，例如：

```text
trade_date
row_count
status
file_path
table_name
S3 object key
小型 dict
```

例如：

```python
return {
    "trade_date": "20260810",
    "row_count": 1000,
    "status": "success",
}
```

---

## XCom 不適合傳什麼？

不建議使用 XCom 傳遞大型資料，例如：

```text
大型 DataFrame
數萬筆股價資料
大型 CSV 內容
圖片或大型 Binary
```

例如不建議：

```python
@task
def extract():
    return large_dataframe
```

原因是 XCom 主要用於 Task 之間交換 metadata 或小型控制資訊，不是大型資料傳輸機制。

---

## 未來如果拆分 ETL

不建議：

```text
Extract
→ XCom 傳整個 DataFrame
→ Transform
→ XCom 傳整個 DataFrame
→ Validate
→ Load
```

比較合理的是：

```text
Extract
→ 將大型資料存到外部 Storage
→ XCom 傳資料位置
→ Transform 根據位置讀取資料
```

例如：

```text
Extract
→ 儲存 raw data 到 S3 / MinIO / Database
→ XCom 傳：
  s3://bucket/raw/20260810.parquet

Transform
→ 根據這個 path 讀取資料
```

所以可以記成：

```text
XCom 傳「資料在哪裡」
而不是傳「整包大型資料」
```

---

## 今日重要理解

```text
XCom
→ Airflow Task 之間的小型資料交換機制。
```

```text
@task 的 return value
→ 可以自動透過 XCom 傳給下游 Task。
```

```text
TaskFlow API
→ 可以直接使用函式參數與 return value 表達 Task 間資料傳遞。
```

```text
XCom 適合小型參數與 metadata，
不適合大型 DataFrame。
```

---

## Day 20 完成內容

```text
[✓] 理解 XCom 的用途
[✓] 使用 XCom 傳遞參數
[✓] 使用上一個 Task 的輸出作為下一個 Task 的輸入
[✓] 理解 @task return value 會透過 XCom 傳遞
[✓] 理解 Task 不需要共享 Python 記憶體
[✓] 理解 XCom 適合小型資料
[✓] 理解大型 DataFrame 不適合透過 XCom 傳遞
```
