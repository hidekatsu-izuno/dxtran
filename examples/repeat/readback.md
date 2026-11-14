# Job Definition

Definition file: ` examples/repeat/readback.yml `

配列を含む固定長・RDW・Parquetの読み戻しを照合する

Inputs: 3 / Processes: 0 / Outputs: 3

An em dash (—) indicates an unspecified setting.

## Parameters

None defined.

## Inputs

### 1. ` fixed_data `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` fixed ` |
| ` path ` | ` out/records.dat ` |

#### ` fixed ` settings

| Setting | Value |
| --- | --- |
| ` record_length ` | ` 25 ` |
| ` record_separator ` | ` lf ` |

#### Fields

| No. | Name | Source name | Type | Offset | Length | Nullable | Additional | Description |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | ` customer_id ` | — | ` string ` | — | ` 4 ` | — | — | 顧客コード |
| 2 | ` item_count ` | — | ` uzoned ` | — | ` 2 ` | — | ` precision: 2 `<br>` zone: ascii ` | 明細件数 |
| 3 | ` monthly_amounts ` | — | ` uzoned ` | — | ` 3 ` | — | ` precision: 3 `<br>` repeat: 2 `<br>` zone: ascii ` | 2か月分の金額 |
| 4 | ` items ` | — | ` group ` | — | — | — | ` repeat: {"count_field":"item_count","max":3,"min":0} ` | 明細件数に応じた商品と数量の配列 |
| 4.1 | ` items[].code ` | — | ` string ` | — | ` 2 ` | — | — | 商品コード |
| 4.2 | ` items[].quantity ` | — | ` uzoned ` | — | ` 2 ` | — | ` precision: 2 `<br>` zone: ascii ` | 数量 |
| 5 | ` trailer ` | — | ` string ` | — | ` 1 ` | — | — | 配列の後続項目 |

### 2. ` rdw_data `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` rdw ` |
| ` path ` | ` out/records.rdw ` |

#### Fields

| No. | Name | Source name | Type | Offset | Length | Nullable | Additional | Description |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | ` customer_id ` | — | ` string ` | — | ` 4 ` | — | — | 顧客コード |
| 2 | ` item_count ` | — | ` uzoned ` | — | ` 2 ` | — | ` precision: 2 `<br>` zone: ascii ` | 明細件数 |
| 3 | ` monthly_amounts ` | — | ` uzoned ` | — | ` 3 ` | — | ` precision: 3 `<br>` repeat: 2 `<br>` zone: ascii ` | 2か月分の金額 |
| 4 | ` items ` | — | ` group ` | — | — | — | ` repeat: {"count_field":"item_count","max":3,"min":0} ` | 明細件数に応じた商品と数量の配列 |
| 4.1 | ` items[].code ` | — | ` string ` | — | ` 2 ` | — | — | 商品コード |
| 4.2 | ` items[].quantity ` | — | ` uzoned ` | — | ` 2 ` | — | ` precision: 2 `<br>` zone: ascii ` | 数量 |
| 5 | ` trailer ` | — | ` string ` | — | ` 1 ` | — | — | 配列の後続項目 |

### 3. ` parquet_data `

| Setting | Value |
| --- | --- |
| ` format ` | ` parquet ` |
| ` path ` | ` out/records.parquet ` |

#### Fields

Uses the input file schema.

## Processes

None defined.

## Outputs

### 1. ` fixed_result `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` csv ` |
| ` mode ` | ` overwrite ` |
| ` path ` | ` out/fixed_readback.csv ` |

#### Fields

Per-field output settings. The SQL result determines the actual columns, column order, and types.

Uses the SQL result columns and types. No per-field settings are defined.

#### SQL

```sql
WITH data AS (SELECT * FROM fixed_data)
SELECT customer_id, item_count, array_length(items) AS actual_count,
       monthly_amounts[2] AS second_month, items[1]['code'] AS first_code, trailer
FROM data
```

### 2. ` rdw_result `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` csv ` |
| ` mode ` | ` overwrite ` |
| ` path ` | ` out/rdw_readback.csv ` |

#### Fields

Per-field output settings. The SQL result determines the actual columns, column order, and types.

Uses the SQL result columns and types. No per-field settings are defined.

#### SQL

```sql
SELECT customer_id, item_count, array_length(items) AS actual_count,
       monthly_amounts[2] AS second_month, items[1]['code'] AS first_code, trailer
FROM rdw_data
```

### 3. ` parquet_result `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` csv ` |
| ` mode ` | ` overwrite ` |
| ` path ` | ` out/parquet_readback.csv ` |

#### Fields

Per-field output settings. The SQL result determines the actual columns, column order, and types.

Uses the SQL result columns and types. No per-field settings are defined.

#### SQL

```sql
SELECT customer_id, item_count, array_length(items) AS actual_count,
       monthly_amounts[2] AS second_month, items[1]['code'] AS first_code, trailer
FROM parquet_data
```

