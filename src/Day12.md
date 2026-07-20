# Day 12 學習筆記：支援單日執行與日期區間回補

今天將原本只能執行單一日期的 TWSE ETL Pipeline，擴充成同時支援單日模式與日期區間模式。

目前 Pipeline 支援兩種執行方式：

```bash
# 單日執行
python src/main.py --trade-date 20240701

# 日期區間執行
python src/main.py --start-date 20240701 --end-date 20240705
```

## 1. 支援單日執行模式

透過 `--trade-date` 指定單一交易日期：

```bash
python src/main.py --trade-date 20240701
```

Pipeline 會針對指定日期依序執行：

```text
Extract
→ Transform
→ Validate
→ Load
```

這種模式適合：

- 每日批次執行
- 重跑特定日期
- 測試單日資料
- 修復某一天的資料

## 2. 支援日期區間模式

新增 `--start-date` 與 `--end-date`，讓 Pipeline 可以處理一段日期範圍：

```bash
python src/main.py \
  --start-date 20240701 \
  --end-date 20240705
```

程式會產生開始日期到結束日期之間的所有日期，並依序執行每日 ETL。

這種模式可以用於歷史資料回補，也就是 Backfill。

常見使用情境包括：

- 補回排程失敗期間的資料
- 第一次建立歷史資料
- 資料清洗邏輯修改後重新處理
- 資料來源修正後重新載入

## 3. 單日與區間模式不能同時使用

程式會檢查執行參數，避免同時傳入：

```bash
python src/main.py \
  --trade-date 20240701 \
  --start-date 20240701 \
  --end-date 20240705
```

因為單日模式和日期區間模式代表兩種不同的執行需求，同時提供會造成執行範圍不明確。

因此，Pipeline 會在正式執行前擋下這類輸入。

## 4. 驗證日期區間順序

日期區間模式會檢查結束日期是否早於開始日期。

例如：

```bash
python src/main.py \
  --start-date 20240710 \
  --end-date 20240701
```

這是不合理的日期範圍，因此程式會在開始呼叫 TWSE API 前停止執行並顯示錯誤。

這符合 Fail Fast 的設計原則：

```text
先驗證輸入
→ 輸入正確後才執行 ETL
```

## 5. 將單日 ETL 抽成 `run_pipeline()`

將原本寫在 `main.py` 中的單日 ETL 流程抽成：

```python
run_pipeline(trade_date)
```

此函式負責執行指定日期的完整流程：

```text
Extract
→ Transform
→ Validate
→ Load
```

單日模式與日期區間模式都會呼叫相同的 `run_pipeline()`，避免重複撰寫兩套 ETL 邏輯。

這樣做的好處包括：

- 單日與區間模式的處理邏輯一致
- 降低重複程式碼
- 修改 ETL 流程時只需要調整一個地方
- 更容易進行測試與維護

## 6. 日期區間逐日呼叫相同 Pipeline

日期區間模式本身不另外實作一套 ETL，而是依序產生每一天的日期，再逐日呼叫：

```python
run_pipeline(trade_date)
```

概念如下：

```text
20240701 → run_pipeline()
20240702 → run_pipeline()
20240703 → run_pipeline()
20240704 → run_pipeline()
20240705 → run_pipeline()
```

因此，日期區間模式可以理解成單日 Pipeline 的批次協調層。

## 7. 非交易日不影響後續日期

日期區間可能包含：

- 星期六
- 星期日
- 國定假日
- 休市日

當 TWSE 沒有回傳資料時，該日期會按照既有空資料處理機制正常結束，而不會中止整個日期區間。

流程可能是：

```text
7 月 5 日：正常取得資料
7 月 6 日：非交易日，沒有資料
7 月 7 日：非交易日，沒有資料
7 月 8 日：正常取得資料
```

即使中間日期沒有資料，後續日期仍然會繼續執行。

這表示：

```text
沒有交易資料
```

屬於可預期的業務情境，不等於 Pipeline 發生系統錯誤。

## 8. 日期區間重跑不會產生重複資料

相同日期區間再次執行時，不會重複寫入相同股票與交易日期的資料。

原因是 PostgreSQL 資料表使用：

```text
stock_id + trade_date
```

作為 unique constraint。

因此，即使進行歷史資料回補或重跑相同日期範圍，Pipeline 仍然可以保持資料結果一致。

這表示 Pipeline 具備 idempotency：

> 同一批輸入資料重複執行，不會產生不同或重複的最終結果。

## 9. 日誌顯示每日與整批執行結果

日期區間執行時，日誌會記錄每一天的處理結果，包括：

```text
Extracted rows
Transformed rows
Validated rows
Loaded rows
```

整個日期區間完成後，也會記錄整批結果，例如：

```text
處理的日期數量
總寫入筆數
批次開始與結束狀態
```

這讓使用者可以同時觀察：

- 每一天處理了多少資料
- 哪一天沒有資料
- 哪一天實際寫入了資料
- 整批共處理多少日期
- 整批總共寫入多少資料

## 今日理解

今天完成的是 Pipeline 的歷史資料回補能力。

整體設計為：

```text
執行參數解析
→ 判斷單日或區間模式
→ 驗證日期與參數組合
→ 產生需要處理的日期
→ 逐日呼叫 run_pipeline()
→ 記錄每日結果
→ 彙總整批結果
```

這次練習也讓我理解到，日期區間模式不需要重新建立一套 ETL，而是應該重複使用既有的單日 Pipeline。

透過共用 `run_pipeline()`，可以讓單日執行、歷史回補與未來的排程執行，都維持相同的資料處理邏輯。
