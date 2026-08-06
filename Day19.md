# Day 19：Airflow Task 依賴與 Trigger Rule

## 一、今日完成內容

今天建立一個獨立的學習 DAG：

```text
task_dependency_demo
```

DAG 中包含三個 Task：

```text
prepare
   ↓
process
   ↓
finish
```

三個 Task 分別負責：

```text
prepare
→ 執行流程開始前的準備工作

process
→ 執行主要處理邏輯

finish
→ 執行流程完成後的工作
```

Task 之間使用：

```python
prepare_task >> process_task >> finish_task
```

設定執行順序。

---

## 二、Task 依賴關係

在以下結構中：

```text
prepare
   ↓
process
   ↓
finish
```

對 `process` 而言：

```text
prepare
→ upstream Task（上游 Task）

finish
→ downstream Task（下游 Task）
```

可以理解成：

```text
Upstream
→ 前一道工作

Downstream
→ 後一道工作
```

Task 依賴決定了工作執行順序。

---

## 三、預設 Trigger Rule：all_success

Airflow Task 預設使用：

```text
all_success
```

代表：

```text
所有直接上游 Task 都執行成功
→ 下游 Task 才會執行
```

例如：

```text
prepare：success
process：success
finish：success
```

如果 `process` 執行失敗：

```text
prepare：success
process：failed
finish：upstream_failed
```

此時 `finish` 不會真正執行。

---

## 四、failed 與 upstream_failed

### failed

```text
Task 本身有開始執行，
但執行過程發生錯誤。
```

例如：

```text
process：failed
```

表示 `process` 已經開始執行，但程式主動拋出例外或發生錯誤。

### upstream_failed

```text
Task 本身沒有執行，
但因為上游 Task 失敗，所以無法開始。
```

例如：

```text
finish：upstream_failed
```

表示 `finish` 自己沒有發生程式錯誤，而是受到 `process` 失敗影響。

因此：

```text
failed
→ Task 自己執行失敗

upstream_failed
→ Task 沒有執行，因上游失敗而被阻擋
```

---

## 五、Trigger Rule：all_done

可以在 `@task` 中設定：

```python
@task(trigger_rule="all_done")
def finish() -> None:
    print("Dependency demo finished")
```

`all_done` 代表：

```text
所有直接上游 Task 都已經結束
→ 下游 Task 就會執行
```

它不要求上游一定成功。

例如：

```text
prepare：success
process：failed
finish：success
```

雖然 `process` 失敗，但因為它已經結束，所以使用 `all_done` 的 `finish` 仍然會執行。

---

## 六、all_success 與 all_done 的差異

### all_success

```text
所有直接上游 Task 都成功
→ 才執行下游 Task
```

適合一般資料處理流程：

```text
資料驗證成功
→ 才能寫入資料庫
```

若驗證失敗，Load 不應繼續執行。

### all_done

```text
所有直接上游 Task 都已結束
→ 不論成功或失敗，都執行下游 Task
```

適合：

```text
清理暫存檔
釋放資源
紀錄批次結束
發送執行結果
```

`all_done` 不適合隨意套用在一般資料處理 Task，否則上游處理失敗後，下游仍可能繼續處理不完整資料。

---

## 七、兩種測試結果

### 正常模式

當 `force_failure=False`：

```text
prepare：success
process：success
finish：success
DAG Run：success
```

### 失敗模式：預設 all_success

當 `force_failure=True`：

```text
prepare：success
process：failed
finish：upstream_failed
DAG Run：failed
```

### 失敗模式：finish 使用 all_done

當 `force_failure=True`：

```text
prepare：success
process：failed
finish：success
DAG Run：failed
```

雖然 `finish` 執行成功，但主要的 `process` Task 仍然失敗，因此整體 DAG Run 仍應視為失敗。

---

## 八、今日重要理解

```text
Task 依賴
→ 決定 Task 的執行順序
```

```text
all_success
→ 所有直接上游成功，下游才執行
```

```text
all_done
→ 所有直接上游結束，不論結果如何，下游都執行
```

```text
failed
→ Task 自己執行後發生錯誤
```

```text
upstream_failed
→ Task 沒有執行，而是被上游失敗影響
```

---

## 九、Day 18 完成內容

```text
[✓] 建立 task_dependency_demo DAG
[✓] 建立 prepare、process、finish 三個 Task
[✓] 使用 >> 建立 Task 依賴
[✓] 理解 upstream 與 downstream
[✓] 理解預設 Trigger Rule 是 all_success
[✓] 觀察 process 失敗後 finish 為 upstream_failed
[✓] 將 finish 設定為 trigger_rule="all_done"
[✓] 觀察上游失敗後 finish 仍然執行
[✓] 理解 failed 與 upstream_failed 的差異
[✓] 沒有修改正式 TWSE ETL DAG
```
