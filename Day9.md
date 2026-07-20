# Day 9 學習筆記：TWSE 原始資料轉換

今天主要練習將 TWSE API 回傳的原始資料，轉換成專案後續 ETL 流程可以使用的標準格式。

## 1. 選取需要的原始欄位

從 TWSE 原始 DataFrame 中選取專案需要的欄位：

- 證券代號
- 證券名稱
- 成交股數
- 開盤價
- 最高價
- 最低價
- 收盤價

原始 API 可能會回傳許多欄位，但專案目前只需要股價與成交量相關資料，因此先選取必要欄位，可以讓後續的轉換邏輯更單純，也能避免不需要的資料進入 Pipeline。

## 2. 將無法轉換的資料改成缺失值

TWSE 原始資料中可能出現 `"--"`，代表該欄位沒有有效資料。

在進行數值型別轉換時，使用：

```python
pd.to_numeric(column, errors="coerce")
```

將 `"--"` 等無法轉換成數字的內容改成 `NaN`。

這樣做的原因是：

- 避免單筆異常資料造成整個程式中斷。
- 不應該直接把 `"--"` 改成 `0`，因為「沒有資料」不代表數值等於 0。
- 缺失值可以在後續的 Validation 階段，再依照資料品質規則決定是否保留或排除。

## 3. 使用 `.copy()` 建立獨立 DataFrame

從原始 DataFrame 選取欄位時，使用：

```python
transformed_df = raw_df[required_columns].copy()
```

先建立一份獨立的 DataFrame，再進行欄位重新命名、型別轉換與資料清洗。

這樣可以：

- 避免直接修改原始 DataFrame。
- 降低 pandas 發生 `SettingWithCopyWarning` 的可能性。
- 讓 raw data 與 transformed data 的責任更加清楚。

## 4. 成交量使用 pandas `Int64` 型別

成交股數原本是整數資料，但資料中可能包含缺失值，因此使用 pandas nullable integer：

```python
.astype("Int64")
```

而不是一般的：

```python
.astype("int64")
```

兩者差異是：

- `int64` 通常不能包含缺失值。
- `Int64` 可以同時保存整數與 `<NA>`。

因此，在資料清洗完成但尚未經過 Validation 前，使用 `Int64` 能夠保留可能存在的缺失資料，再交由後續流程處理。

## 今日理解

今天完成的是 TWSE raw data 到專案標準資料格式之間的轉換。

整體流程可以理解成：

```text
TWSE 原始資料
→ 選取必要欄位
→ 複製 DataFrame
→ 清除格式問題
→ 將無效數值轉成缺失值
→ 轉換資料型別
→ 交給 Validation 層檢查
```

Transform 階段的責任是將資料整理成統一且可處理的格式；至於資料是否符合業務規則，則應該交由 Validation 階段判斷。
