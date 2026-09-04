# Day 27｜Airflow Sensor：`poke` vs `reschedule`

## 今日主題

Day 26 已經學會 Sensor 的基本概念：

```text
Sensor
→ 等待條件成立

poke_interval
→ 多久檢查一次

timeout
→ 最多允許等待多久
```

Day 27 進一步處理一個 Production Airflow 很重要的問題：

> **Sensor 等待條件成立的期間，Worker 資源要怎麼處理？**

今天主要比較：

```text
mode="poke"

vs

mode="reschedule"
```

核心差異：

```text
poke
→ 等待期間持續占用 Worker Slot

reschedule
→ 條件未成立時釋放 Worker Slot
→ 之後再重新排程檢查
```

---

# 1. 為什麼需要理解 Sensor Mode？

Day 26 的 Demo 只等待幾秒鐘。

但真實 Data Pipeline 可能需要等待：

```text
上游 Batch 完成
每日資料檔到達
Database 資料 Ready
另一個 Pipeline 完成
```

例如：

```text
02:00
Airflow Pipeline 啟動
↓
等待上游資料
↓
上游可能 03:30 才完成
```

代表 Sensor 可能需要等待：

```text
1～2 小時
```

這時就要考慮：

> Sensor 等待的這 1～2 小時，要不要一直占著 Worker Slot？

這就是：

```text
poke
vs
reschedule
```

要解決的問題。

---

# 2. Worker Slot 是什麼？

可以先用簡化方式理解：

> Worker Slot 代表 Airflow Worker 可以同時拿來執行 Task 的容量。

假設 Worker 同時只能處理四個 Task：

```text
Worker

Slot 1
Slot 2
Slot 3
Slot 4
```

如果四個 Slot 都被 Sensor 占用：

```text
Slot 1 → Sensor A
Slot 2 → Sensor B
Slot 3 → Sensor C
Slot 4 → Sensor D
```

而 Sensor 都只是在：

```text
等待上游資料...
```

其他真正需要執行的工作：

```text
Extract
Transform
Validate
Load
```

就可能需要等待可用的 Worker Capacity。

因此：

> 長時間等待時，Sensor 如何使用 Worker Slot 是重要的資源管理問題。

---

# 3. `poke` Mode

Sensor 可以設定：

```python
mode="poke"
```

概念：

```text
取得 Worker Slot
↓
Check Condition
↓
False
↓
等待 poke_interval
↓
再次 Check
↓
False
↓
繼續等待
```

最重要的是：

> **Sensor 在等待期間仍然持續占用 Worker Slot。**

流程：

```text
Worker Slot
    ↓
poke Sensor
    ↓
Check
    ↓
False
    ↓
Sleep / Wait
    ↓
仍然占用 Worker Slot
    ↓
Check Again
```

直到：

```text
Condition = True
```

或：

```text
Sensor Timeout
```

才結束這段等待。

---

# 4. `poke` 的白話理解

可以把 `poke` 想成：

> **「我坐在這個位置等，等一下再問一次。」**

例如：

```text
Sensor：
「檔案到了嗎？」

No

Sensor：
「好，我坐在這裡等。」

5 分鐘後：

「檔案到了嗎？」

No

「那我繼續坐在這裡。」
```

所以：

```text
等待期間
↓
Worker Slot 仍然被占用
```

---

# 5. `reschedule` Mode

另一種模式：

```python
mode="reschedule"
```

當條件沒有成立時：

```text
取得 Worker Slot
↓
Check
↓
False
↓
釋放 Worker Slot
↓
等待下一個檢查時間
↓
重新排程
↓
再次取得 Worker Slot
↓
Check Again
```

核心：

> **條件尚未 Ready 時，不需要一直占著 Worker Slot。**

---

# 6. `reschedule` 的白話理解

可以把它想成：

> **「現在還沒好，我先把位置讓給別人，等等再回來檢查。」**

例如：

```text
Sensor：
「檔案到了嗎？」

No

Sensor：
「那我先離開。」

↓
釋放 Worker Slot

其他 Task 使用 Worker
↓
Extract
Transform
Load

下一個檢查時間
↓
Sensor 再回來
↓
重新取得 Worker Slot
↓
「檔案到了嗎？」
```

這就是 `reschedule` 的核心價值。

---

# 7. `poke` vs `reschedule`

| 比較       | `poke`                   | `reschedule`            |
| ---------- | ------------------------ | ----------------------- |
| 條件 False | 等待後再檢查             | 釋放 Slot，之後重新排程 |
| 等待期間   | 占用 Worker Slot         | 釋放 Worker Slot        |
| 短時間等待 | 適合                     | 可以，但未必必要        |
| 長時間等待 | 容易浪費 Worker Capacity | 通常較適合              |
| 核心概念   | 坐著等                   | 先離開，等等再回來      |

最簡單的記法：

```text
poke
→ 我坐在這裡等

reschedule
→ 我先把位置讓出來
→ 等等再回來檢查
```

---

# 8. `reschedule` 不等於 Failure

這是今天很重要的觀念。

當 Sensor：

```text
Check
↓
False
```

使用 `reschedule` 時：

```text
False
↓
條件尚未 Ready
↓
釋放 Worker Slot
↓
等待重新排程
```

這不代表：

```text
Sensor Failed
```

因此：

```text
reschedule
≠ Failure
```

更精確來說：

> Sensor 還沒有完成，只是目前條件尚未成立，所以暫時釋放執行資源，等待之後再次檢查。

---

# 9. Day 26 的 False 再複習

Sensor Condition：

```text
False
```

代表：

```text
Not Ready
```

而不是：

```text
Failure
```

所以：

```text
False
≠ Failed

reschedule
≠ Failed
```

真正可能導致 Sensor Failed 的情況，例如：

```text
Sensor Timeout
```

或執行過程發生真正的 Exception。

---

# 10. `poke_interval` 在兩種 Mode 中的角色

假設：

```python
poke_interval=300
```

也就是約：

```text
5 分鐘
```

### poke

```text
Check
↓
False
↓
持續占用 Worker Slot
↓
等待約 5 分鐘
↓
Check Again
```

### reschedule

```text
Check
↓
False
↓
釋放 Worker Slot
↓
等待下一次檢查
↓
重新取得 Worker Slot
↓
Check Again
```

兩者都會：

```text
再次檢查
```

差別在於：

> **等待下一次檢查期間是否持續占用 Worker Slot。**

---

# 11. 為什麼長時間等待不適合一直使用 `poke`？

假設：

```text
上游資料可能晚 2 小時
```

使用：

```text
mode="poke"
```

可能形成：

```text
Sensor
↓
等待 2 小時
↓
Worker Slot
持續被占用
```

如果同時有很多 Sensor：

```text
Sensor A → 占 Slot
Sensor B → 占 Slot
Sensor C → 占 Slot
Sensor D → 占 Slot
```

就可能影響：

```text
Extract
Transform
Validate
Load
其他 DAG
```

取得 Worker Capacity。

因此長時間等待使用 `poke`：

> 可能造成 Worker 資源利用效率下降。

---

# 12. 為什麼長時間等待適合 `reschedule`？

例如：

```text
等待上游資料 1～2 小時
```

使用 `reschedule`：

```text
Check
↓
Not Ready
↓
Release Worker Slot
↓
其他 Task 可以使用 Worker
↓
下一個檢查時間
↓
Sensor 再回來
```

因此：

```text
長時間等待
↓
reschedule
↓
Worker Slot 可以被其他 Task 使用
```

通常比一直占用 Worker Slot 更合理。

---

# 13. 實際 Data Pipeline 情境

假設金融 Batch：

```text
02:00
下游 Pipeline 啟動
↓
Sensor 檢查上游資料
↓
Not Ready
```

但上游可能：

```text
03:00～04:00
```

才完成。

### 使用 `poke`

```text
02:00
取得 Worker Slot
↓
Check → False
↓
等待
↓
Check
↓
等待
↓
Check
↓
...
↓
03:30 Ready
```

這段期間 Worker Slot 持續被 Sensor 占用。

### 使用 `reschedule`

```text
02:00
取得 Worker Slot
↓
Check → False
↓
釋放 Worker Slot

02:10
重新取得 Worker Slot
↓
Check → False
↓
再次釋放

...

03:30
重新取得 Worker Slot
↓
Check → True
↓
Sensor Success
↓
開始 ETL
```

這樣等待期間的 Worker Capacity 可以留給其他 Task。

---

# 14. 今天的 Demo DAG

新增：

```text
airflow/dags/sensor_mode_demo_dag.py
```

DAG：

```text
sensor_mode_demo
```

概念：

```text
        sensor_mode_demo
          /          \
         ↓            ↓
   poke_sensor   reschedule_sensor
```

兩個 Sensor 都用來觀察：

```text
Condition False
```

時的等待行為。

主要比較：

```text
poke_sensor
→ 等待期間占用 Worker Slot

reschedule_sensor
→ 條件未成立後釋放 Worker Slot
```

---

# 15. 今天不是在比較誰比較快

這點很重要。

`reschedule` 的核心目的不是：

```text
執行速度比較快
```

而是：

```text
等待期間
↓
更有效率地使用 Worker Capacity
```

所以今天比較的是：

> **Resource Usage**

而不是：

> **Execution Speed**

---

# 16. 什麼時候比較適合 `poke`？

如果等待時間非常短：

```text
幾秒
幾十秒
短時間內高度可能 Ready
```

可以考慮：

```text
poke
```

因為流程簡單：

```text
Check
↓
短暫等待
↓
Check
↓
Ready
```

不一定需要為短時間等待一直進行重新排程。

---

# 17. 什麼時候比較適合 `reschedule`？

如果等待：

```text
數十分鐘
1 小時
2 小時
甚至更久
```

例如：

```text
等待上游 Batch
等待每日檔案
等待其他 Pipeline
等待 Database 資料 Ready
```

通常會比較傾向：

```text
reschedule
```

原因：

> 等待期間可以釋放 Worker Slot，避免長時間占用執行資源。

---

# 18. `poke` / `reschedule` 與 Timeout

不論使用：

```text
poke
```

或：

```text
reschedule
```

Sensor 還是應該有合理的：

```text
timeout
```

因為：

```text
釋放 Worker Slot
≠
可以永遠等待
```

例如：

```text
Sensor
↓
Not Ready
↓
Reschedule
↓
Not Ready
↓
Reschedule
↓
...
↓
超過 Timeout
↓
Failed
```

因此：

```text
mode
→ 等待期間怎麼使用 Worker 資源

timeout
→ 最多允許等待多久
```

兩者處理的是不同問題。

---

# 19. Day 26 + Day 27 串聯

Day 26：

```text
Sensor
↓
Condition Ready？
├─ No → 等待
└─ Yes → Success
```

Day 27：

```text
如果要等待
↓
Worker Slot 怎麼辦？

├─ poke
│    ↓
│  繼續占用
│
└─ reschedule
     ↓
   釋放 Worker Slot
     ↓
   之後再回來檢查
```

所以：

```text
Day 26
→ 怎麼等待

Day 27
→ 等待期間怎麼管理 Worker 資源
```

---

# 20. Sensor / Retry / Failure 再比較

現在可以把三個容易混淆的概念分開。

## Sensor False

```text
Condition Not Ready
↓
等待
```

不是 Failure。

## Reschedule

```text
Condition Not Ready
↓
釋放 Worker Slot
↓
之後再次檢查
```

不是 Failure。

## Retry

```text
Task 已經執行
↓
發生 Failure
↓
重新嘗試
```

因此：

```text
Sensor False
≠ Failure

Reschedule
≠ Failure

Retry
→ 發生 Failure 後重新嘗試
```

---

# 21. Day 24～27 整體流程

目前可以組成：

```text
Pipeline Start
      ↓
    Sensor
      ↓
Condition Ready？
 │           │
No          Yes
 │           │
等待         ↓
 │        Task 執行
 │           ↓
 │       執行成功？
 │        │       │
 │       Yes      No
 │        │       │
 │     Success   Failure
 │                ↓
 │             Retry？
 │             │     │
 │            Yes    No
 │             │     │
 │          Retry   Final Failed
 │                   ↓
 │          on_failure_callback
 │
 └─ 等待方式
      │
      ├─ poke
      │   → 占用 Worker Slot
      │
      └─ reschedule
          → 釋放 Worker Slot
          → 之後重新檢查
```

---

# 22. 更進階：Deferrable Sensor

Production Airflow 還有更進一步的：

```text
Deferrable Operators
Deferrable Sensors
```

核心方向同樣是：

> 長時間等待時，不要浪費 Worker Slot。

概念上：

```text
Task 開始
↓
需要等待外部事件
↓
Defer
↓
釋放 Worker
↓
由 Triggerer 等待事件
↓
事件成立
↓
Task 恢復
```

但 Day 27：

```text
不實作 Deferrable Sensor
```

目前只需要知道它存在，以及為什麼 Airflow 需要這類機制。

---

# 23. 今日實際學會

## 1. `poke` 等待期間會不會占用 Worker Slot？

會。

```text
poke
↓
Check
↓
False
↓
等待
↓
仍然占用 Worker Slot
```

---

## 2. `reschedule` 條件不成立時怎麼處理？

```text
Check
↓
False
↓
釋放 Worker Slot
↓
等待下一次檢查時間
↓
重新排程
↓
再次取得 Worker Slot
↓
Check Again
```

注意：

> 這裡不是 Sensor Task 已經完成，而是條件尚未成立，因此暫時釋放執行資源。

---

## 3. 為什麼長時間等待不適合一直使用 `poke`？

因為：

```text
長時間等待
↓
長時間占用 Worker Slot
↓
降低 Worker Capacity
↓
其他 ETL Task 可能需要等待資源
```

---

## 4. `reschedule` 是否代表 Failure？

不是。

```text
reschedule
→ Not Ready

Failure
→ Task 執行失敗
```

兩者不同。

---

## 5. 上游可能晚 1～2 小時，選哪一個？

通常比較傾向：

```text
reschedule
```

因為：

```text
等待時間長
↓
不需要一直占用 Worker Slot
↓
Worker 可以執行其他 Task
```

---

# Day 27 核心整理

```text
Sensor
↓
Check Condition
↓
False
↓
怎麼等待？

        ┌──────────────┐
        │              │
      poke        reschedule
        │              │
        ↓              ↓
持續占用 Slot      釋放 Slot
        │              │
等待 interval      等待重新排程
        │              │
        ↓              ↓
再次 Check        再取得 Slot
                       ↓
                   再次 Check
```

最重要的四句：

```text
poke
→ 等待期間持續占用 Worker Slot

reschedule
→ 條件未成立時釋放 Worker Slot

reschedule
≠ Failure

長時間等待
→ 通常更傾向使用 reschedule
```

以及 Day 27 最重要的一句：

> **Day 26 學「怎麼等待」，Day 27 學「等待時不要浪費 Worker 資源」。**

## Day 27 完成

**Airflow Sensor `poke` vs `reschedule` / Worker Resource Management ✅**
