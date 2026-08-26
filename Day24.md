# Day 24｜Airflow Task Timeout / Retry

## 今日目標

學習 Airflow 如何透過：

- `execution_timeout`
- `retries`
- `retry_delay`

處理 Task 執行過久與暫時性失敗。

核心概念：

```text
execution_timeout
→ 單次 Attempt 最長可以執行多久

Retry
→ Task Failure 後是否重新嘗試

retries
→ 最多允許額外重試幾次
```

---

## 1. 為什麼需要 Timeout？

Pipeline 不一定會直接發生 Error。

有時可能出現：

```text
Task 開始執行
      ↓
沒有 Error
      ↓
但也一直沒有完成
      ↓
Running...
```

例如：

```text
TWSE API 沒有正常回應
Database Query 卡住
Network Connection 卡住
外部服務沒有回應
```

如果沒有執行時間限制，Task 可能長時間停留在：

```text
running
```

因此需要設定：

```python
execution_timeout
```

限制 Task 的最大執行時間。

---

# 2. `execution_timeout`

例如：

```python
@task(
    execution_timeout=timedelta(seconds=5)
)
def slow_task():
    ...
```

代表：

> 這個 Task 的單次 Attempt 最多允許執行 5 秒。

如果：

```text
執行時間 < 5 秒
```

Task 可以正常完成：

```text
success
```

如果：

```text
執行時間 > 5 秒
```

則：

```text
Timeout
↓
這次 Attempt Failure
```

---

# 3. `execution_timeout` 限制的是單次 Attempt

這是 Day 24 最重要的觀念之一。

假設：

```python
execution_timeout=timedelta(seconds=5)
```

限制的是：

```text
Attempt 1
→ 最多 5 秒

Attempt 2
→ 又有自己的 5 秒

Attempt 3
→ 又有自己的 5 秒
```

不是：

```text
三次 Attempt
總共只能執行 5 秒
```

因此：

> `execution_timeout` 是每一次 Task Attempt 的執行時間限制。

---

# 4. Quick Task 實驗

設定：

```python
@task(
    execution_timeout=timedelta(seconds=5)
)
def quick_task():
    print("Quick task started.")

    time.sleep(2)

    print("Quick task finished.")
```

條件：

```text
實際執行時間 = 2 秒
Timeout = 5 秒
```

因為：

```text
2 < 5
```

所以：

```text
quick_task
→ success
```

---

# 5. Slow Task 實驗

設定：

```python
@task(
    execution_timeout=timedelta(seconds=5)
)
def slow_task():
    print("Slow task started.")

    time.sleep(10)

    print("Slow task finished.")
```

條件：

```text
Task 預計執行 = 10 秒
Timeout = 5 秒
```

因為：

```text
10 > 5
```

所以 Airflow 會在超過允許時間後讓這次 Attempt 因 Timeout 而失敗：

```text
slow_task
↓
Timeout
↓
Failed
```

---

# 6. Retry

Retry 用來處理 Task Failure 後的重新嘗試。

例如：

```python
@task(
    retries=2
)
```

代表：

> 第一次執行失敗後，最多允許再重新嘗試 2 次。

因此：

```text
第一次執行
+
Retry 2 次
=
最多 3 Attempts
```

---

# 7. `retries=2` 不代表總共執行兩次

這是很重要的區別。

```python
retries=2
```

不是：

```text
Attempt 1
Attempt 2

總共 2 次
```

而是：

```text
Attempt 1
↓ Failed

Retry 1
Attempt 2
↓ Failed

Retry 2
Attempt 3
↓ Failed

最終 Failed
```

因此：

```text
retries
=
第一次執行以外
允許額外重試的次數
```

---

# 8. Retry 不代表一定要把次數跑完

假設：

```python
retries=2
```

第一次失敗：

```text
Attempt 1
→ Failed
```

第二次成功：

```text
Attempt 2
→ Success
```

流程就會結束。

不會再執行：

```text
Attempt 3
```

因此 `retries=2` 代表：

```text
最多額外 Retry 2 次
```

而不是：

```text
一定 Retry 2 次
```

---

# 9. Failure 與 Retry

Task 發生 Failure 後，不代表一定會 Retry。

如果：

```text
沒有設定 Retry
```

可能直接：

```text
Task
↓
Failed
↓
結束
```

如果設定：

```python
retries=2
```

而且還有剩餘 Retry 次數：

```text
Task
↓
Failed
↓
還有 Retry
↓
重新執行
```

因此更精確的理解是：

> Task Failure 後，如果有設定 Retry，而且仍有剩餘 Retry 次數，Airflow 才會重新嘗試。

---

# 10. `retry_delay`

除了：

```python
retries=2
```

還可以設定：

```python
retry_delay=timedelta(seconds=3)
```

代表：

```text
Attempt Failure
      ↓
等待 3 秒
      ↓
下一次 Attempt
```

例如：

```text
Attempt 1
↓ Failed
等待 3 秒
↓
Attempt 2
```

這樣可以避免外部服務暫時發生問題時，Airflow 馬上連續重新請求。

---

# 11. Timeout + Retry

兩者可以一起使用：

```python
@task(
    execution_timeout=timedelta(seconds=5),
    retries=2,
    retry_delay=timedelta(seconds=3),
)
def slow_task():
    time.sleep(10)
```

流程：

```text
Attempt 1
↓
超過 5 秒
↓
Timeout / Failure
↓
等待 3 秒
↓
Attempt 2
↓
超過 5 秒
↓
Timeout / Failure
↓
等待 3 秒
↓
Attempt 3
↓
超過 5 秒
↓
Timeout / Failure
↓
沒有剩餘 Retry
↓
最終 Failed
```

因此：

```text
execution_timeout
→ 控制每次 Attempt 最長跑多久

retry_delay
→ Failure 後多久再試

retries
→ 最多允許額外再試幾次
```

---

# 12. Timeout 與 Retry 的責任不同

### `execution_timeout`

解決：

```text
Task 一直 Running
沒有結束
```

控制：

```text
單次 Attempt 最長執行時間
```

---

### `retries`

解決：

```text
Task 發生 Failure
是否值得重新嘗試
```

控制：

```text
Failure 後最多額外重新執行幾次
```

---

### `retry_delay`

控制：

```text
兩次 Attempt 之間
需要等待多久
```

---

# 13. Retry 適合什麼錯誤？

Retry 比較適合暫時性問題，例如：

```text
Network Error
API Connection Error
Database Connection Error
Temporary Service Error
Timeout
```

因為：

```text
第一次失敗
≠
第二次一定失敗
```

稍後重新執行可能恢復正常。

---

# 14. Retry 不適合什麼錯誤？

例如：

```text
Syntax Error
Import Error
錯誤參數
明確 Business Logic Error
```

這些問題通常：

```text
Attempt 1
→ Failed

Attempt 2
→ 還是 Failed

Attempt 3
→ 還是 Failed
```

因為程式本身沒有被修正。

所以：

> 不是所有 Failure 都值得 Retry。

---

# 15. Airflow Timeout 與 HTTP Timeout

未來 TWSE Pipeline 可能同時存在兩種 Timeout。

例如：

```python
requests.get(
    url,
    timeout=10
)
```

這是：

```text
HTTP Request Timeout
```

控制：

> 單次 HTTP Request 最多等待多久。

而：

```python
execution_timeout=timedelta(minutes=5)
```

是：

```text
Airflow Task Timeout
```

控制：

> 整個 Task Attempt 最多執行多久。

因此：

```text
HTTP Timeout
→ Request 層級

execution_timeout
→ Airflow Task Attempt 層級
```

兩者不是同一件事。

---

# 16. 完整 Failure Protection 概念

未來 Pipeline 可能形成：

```text
Airflow Task
│
├── HTTP Request Timeout
│      ↓
│   控制 API Request 等待時間
│
├── execution_timeout
│      ↓
│   控制整個 Task Attempt 執行時間
│
├── retries
│      ↓
│   控制 Failure 後最多重試幾次
│
└── retry_delay
       ↓
    控制 Retry 間隔
```

例如：

```text
HTTP Request Timeout
= 10 秒

Airflow execution_timeout
= 5 分鐘

retries
= 2

retry_delay
= 1 分鐘
```

---

# 17. 本日 Demo

新增：

```text
airflow/dags/timeout_demo_dag.py
```

DAG：

```text
timeout_demo
```

Dependency：

```text
quick_task
    ↓
slow_task
```

第一階段：

```text
quick_task
sleep 2 秒
timeout 5 秒
→ Success

slow_task
sleep 10 秒
timeout 5 秒
→ Timeout / Failed
```

第二階段加入：

```text
retries=2
retry_delay=3 秒
```

觀察：

```text
Timeout
↓
Retry
↓
Timeout
↓
Retry
↓
Timeout
↓
最終 Failed
```

---

# 18. Day 24 今天學會

1. `execution_timeout` 可以限制單次 Task Attempt 的最大執行時間。

2. Task 執行時間超過 `execution_timeout` 時，該次 Attempt 會因 Timeout 而失敗。

3. Retry 是 Task Failure 後的重新嘗試機制。

4. Task Failure 後必須有設定 Retry，而且仍有剩餘 Retry 次數，Airflow 才會重新嘗試。

5. `retries=2` 代表第一次執行之外，最多允許額外 Retry 兩次。

6. 因此：

```text
retries=2
→ 最多 3 Attempts
```

7. 如果 Retry 過程中已經 Success，就不會繼續執行剩餘 Retry。

8. 每一次 Retry 都是一個新的 Attempt，每個 Attempt 都重新受到自己的 `execution_timeout` 限制。

9. Retry 適合 Network、API、DB Connection、Timeout 等暫時性錯誤。

10. Syntax Error、Import Error 等確定性程式錯誤，即使 Retry 通常也不會自行恢復。

---

# Day 24 核心整理

```text
Task Attempt
     ↓
執行時間超過 execution_timeout？
     │
 ┌───┴────┐
 No       Yes
 │         │
Success   Timeout
           ↓
         Failure
           ↓
      還有 Retry？
       │       │
      Yes      No
       │       │
等待 retry_delay
       │       │
新的 Attempt  最終 Failed
```

最重要的三個設定：

```text
execution_timeout
→ 每次 Attempt 最多執行多久

retries
→ Failure 後最多額外再試幾次

retry_delay
→ Retry 之前等待多久
```

最終記憶：

```text
retries=2

第一次執行
+
最多重試 2 次
=
最多 3 Attempts
```

**Day 24：Airflow Task Timeout / Retry 完成。**
