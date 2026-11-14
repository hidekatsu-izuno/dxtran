# Job Definition

Definition file: ` examples/repeat/conditional_readback.yml `

条件付き配列と残余項目を含むRDWの読み戻し

Inputs: 1 / Processes: 0 / Outputs: 1

An em dash (—) indicates an unspecified setting.

## Parameters

None defined.

## Inputs

### 1. ` data `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` rdw ` |
| ` path ` | ` out/conditional.rdw ` |

#### Fields

| No. | Name | Source name | Type | Offset | Length | Nullable | Additional | Description |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | ` record_type ` | — | ` string ` | — | ` 1 ` | — | — |  |
| 2 | ` item_count ` | — | ` uint-be ` | — | ` 1 ` | — | — |  |
| 3 | ` items ` | — | ` group ` | — | ` 5 ` | — | ` repeat: {"count_field":"item_count","max":3,"min":0} `<br>` when: record_type = 'D' ` |  |
| 3.1 | ` items[].kind ` | — | ` string ` | — | ` 1 ` | — | — |  |
| 3.2 | ` items[].text ` | — | ` string ` | ` 1 ` | ` rest ` | — | ` max_length: 4 `<br>` when: kind = 'T' ` |  |
| 3.3 | ` items[].raw ` | — | ` binary ` | ` 1 ` | ` rest ` | — | ` max_length: 4 `<br>` when: kind = 'B' ` |  |
| 4 | ` note ` | — | ` string ` | — | ` rest ` | — | ` max_length: 100 ` |  |

## Processes

None defined.

## Outputs

### 1. ` summary `

| Setting | Value |
| --- | --- |
| ` encoding ` | ` utf-8 ` |
| ` format ` | ` csv ` |
| ` mode ` | ` overwrite ` |
| ` path ` | ` out/conditional_readback.csv ` |

#### Fields

Per-field output settings. The SQL result determines the actual columns, column order, and types.

Uses the SQL result columns and types. No per-field settings are defined.

#### SQL

```sql
SELECT record_type, item_count, items IS NULL AS items_absent,
       array_length(items) AS actual_count,
       items[1]['text'] AS first_text,
       items[2]['raw'] IS NOT NULL AS second_is_binary, note
FROM data
```

