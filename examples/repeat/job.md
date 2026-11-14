# Job Definition

Definition file: ` examples/repeat/job.yml `

固定回数と件数項目による可変回数の繰り返しを読み書きする

Inputs: 1 / Processes: 0 / Outputs: 5

An em dash (—) indicates an unspecified setting.

## Parameters

None defined.

## Inputs

### 1. ` data `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` fixed ` |
| ` path ` | ` data/records.dat ` |

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

#### ` check `

| No. | From | Condition (when) | Level | Message | Description |
| --- | --- | --- | --- | --- | --- |
| 1 | — | ` array_length(items) <> item_count ` | ` error ` | ` 明細件数が一致しません ` |  |

## Processes

None defined.

## Outputs

### 1. ` fixed_result `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` fixed ` |
| ` mode ` | ` overwrite ` |
| ` path ` | ` out/records.dat ` |

#### ` fixed ` settings

| Setting | Value |
| --- | --- |
| ` record_length ` | ` 25 ` |
| ` record_separator ` | ` lf ` |

#### Fields

| No. | Name | Source name | Type | Offset | Length | Nullable | Additional | Description |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | ` customer_id ` | — | ` Inherited from SQL result ` | — | ` 4 ` | — | — | 顧客コード |
| 2 | ` item_count ` | — | ` uzoned ` | — | ` 2 ` | — | ` precision: 2 `<br>` zone: ascii ` | 明細件数 |
| 3 | ` monthly_amounts ` | — | ` uzoned ` | — | ` 3 ` | — | ` precision: 3 `<br>` repeat: 2 `<br>` zone: ascii ` | 2か月分の金額 |
| 4 | ` items ` | — | ` group ` | — | — | — | ` repeat: {"count_field":"item_count","max":3,"min":0} ` | 明細件数に応じた商品と数量の配列 |
| 4.1 | ` items[].code ` | — | ` Inherited from SQL result ` | — | ` 2 ` | — | — | 商品コード |
| 4.2 | ` items[].quantity ` | — | ` uzoned ` | — | ` 2 ` | — | ` precision: 2 `<br>` zone: ascii ` | 数量 |
| 5 | ` trailer ` | — | ` Inherited from SQL result ` | — | ` 1 ` | — | — | 配列の後続項目 |

#### SQL

```sql
SELECT * FROM data
```

### 2. ` rdw_result `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` rdw ` |
| ` mode ` | ` overwrite ` |
| ` path ` | ` out/records.rdw ` |

#### Fields

| No. | Name | Source name | Type | Offset | Length | Nullable | Additional | Description |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | ` customer_id ` | — | ` Inherited from SQL result ` | — | ` 4 ` | — | — | 顧客コード |
| 2 | ` item_count ` | — | ` uzoned ` | — | ` 2 ` | — | ` precision: 2 `<br>` zone: ascii ` | 明細件数 |
| 3 | ` monthly_amounts ` | — | ` uzoned ` | — | ` 3 ` | — | ` precision: 3 `<br>` repeat: 2 `<br>` zone: ascii ` | 2か月分の金額 |
| 4 | ` items ` | — | ` group ` | — | — | — | ` repeat: {"count_field":"item_count","max":3,"min":0} ` | 明細件数に応じた商品と数量の配列 |
| 4.1 | ` items[].code ` | — | ` Inherited from SQL result ` | — | ` 2 ` | — | — | 商品コード |
| 4.2 | ` items[].quantity ` | — | ` uzoned ` | — | ` 2 ` | — | ` precision: 2 `<br>` zone: ascii ` | 数量 |
| 5 | ` trailer ` | — | ` Inherited from SQL result ` | — | ` 1 ` | — | — | 配列の後続項目 |

#### SQL

```sql
SELECT * FROM data
```

### 3. ` parquet_result `

| Setting | Value |
| --- | --- |
| ` format ` | ` parquet ` |
| ` mode ` | ` overwrite ` |
| ` path ` | ` out/records.parquet ` |

#### Fields

Uses the SQL result columns and types. No per-field settings are defined.

#### SQL

```sql
SELECT * FROM data
```

### 4. ` summary `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` csv ` |
| ` mode ` | ` overwrite ` |
| ` path ` | ` out/summary.csv ` |

#### Fields

Per-field output settings. The SQL result determines the actual columns, column order, and types.

Uses the SQL result columns and types. No per-field settings are defined.

#### SQL

```sql
SELECT customer_id, item_count, array_length(items) AS actual_count, monthly_amounts[2] AS second_month, items[1]['code'] AS first_code, trailer FROM data
```

### 5. ` details `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` csv ` |
| ` mode ` | ` overwrite ` |
| ` path ` | ` out/details.csv ` |

#### Fields

Per-field output settings. The SQL result determines the actual columns, column order, and types.

Uses the SQL result columns and types. No per-field settings are defined.

#### SQL

```sql
SELECT customer_id, item['code'] AS code, item['quantity'] AS quantity
FROM (SELECT customer_id, UNNEST(items) AS item FROM data) expanded
```

