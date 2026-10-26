# 入出力の組み合わせサンプル

固定長・CSV・Parquet の変換を、入力ファイル、実行用 YAML、期待ファイルとともに収録しています。14ディレクトリに25ジョブ（正常系20、異常系5）があります。

| ディレクトリ | 入力 → 出力 | 確認する内容 |
| --- | --- | --- |
| [csv_to_csv](csv_to_csv/job.yml) | CSV → CSV / TSV | 先頭ゼロ、NULL と空文字、引用符・カンマ・項目内改行、最終改行なし、Decimal、真偽値、日付、hex → Base64、列別引用指定 |
| [csv_to_fixed](csv_to_fixed/job.yml) | CSV → 固定長 → CSV | UTF-8 日本語のバイト幅、LF、明示 offset、FILLER、右寄せ、NULL 補完、読み戻し |
| [fixed_to_csv](fixed_to_csv/job.yml) | 固定長 → CSV / TSV | CP932 → UTF-8 / CP932、CRLF、末尾区切り省略、ASCII 符号付きゾーン10進数、正規表現による NULL 判定 |
| [fixed_to_fixed](fixed_to_fixed/job.yml) | 固定長 → 固定長 → CSV | CP932 → UTF-16LE / UTF-8、CRLF → LF / 区切りなし、全角空白による幅埋め、UTF-16 の改行・読み戻し |
| [csv_dialects](csv_dialects/job.yml) | CSV → CSV → CSV | ヘッダーなし、セミコロン区切り、単一引用符、CP932、UTF-16LE TSV、ISO-2022-JP、BOM 付き UTF-16BE の入力 |
| [binary_fields](binary_fields/job.yml) | 固定長 → CSV / 固定長 | 全23型、符号・エンディアン、IEEE / IBM HFP（`hfloat-le` / `hfloat-be`）、EBCDIC / ASCII ゾーン、パック10進数、ビット順の変更、生バイト一致 |
| [csv_to_binary](csv_to_binary/job.yml) | CSV → 固定長 → CSV | 全23型の固定長出力、SQL CAST、負数・ゼロ・暗黙小数点、読み戻し |
| [null_defaults](null_defaults/job.yml) | CSV → 固定長 | 全23型の NULL 補完バイト、NULL と true が混在するビット領域 |
| [datetime](datetime/job.yml) | CSV → 固定長 / CSV → CSV | 2桁年の1970〜2069年への復元、ナノ秒、UTC / Asia/Tokyo、タイムゾーンなし日時、`Z` の `+0900` 表記 |
| [join_aggregate](join_aggregate/job.yml) | 固定長 + CSV → CSV + 固定長 | フィルタ、JOIN、該当マスターなし、元の明細順、SUM / COUNT、明示ソート、複数出力 |
| [empty](empty/job.yml) | CSV / 固定長 → CSV / 固定長 | 0件入力、ヘッダーのみ、ヘッダーなし、0バイトの固定長出力 |
| [parquet](parquet/job.yml) | CSV → Parquet → CSV | スキーマ・Decimal・バイナリ・NULL・空文字・改行・元順の保持、Zstd圧縮、fields / encoding なしの読み戻し |
| [compression](compression/job.yml) | gzip CSV / 固定長 → gzip / zstd → CSV | 明示したcompression、UTF-8 / CP932、NULL・空文字・Decimal・バイナリ・項目内改行、4種類の圧縮出力の読み戻し |
| [errors](errors) | CSV / 固定長 → エラー | 閉じていない引用符、短い固定長レコード、文字幅超過、scale による精度喪失、CP932 に変換できない文字 |

各サンプルの `data/` が入力、`expected/` が期待値、実行で生成される `out/` が出力です。固定長の `.hex` はバイト列を表示する補助ファイルです。`readback.yml` がある場合は `job.yml` の後に実行します。全23型の共通レイアウトは [layouts/all_types.yml](layouts/all_types.yml) で、1レコード132バイトです。

## 個別に実行する

リポジトリのルートで次を実行します。出力先ディレクトリは事前に作成してください。

```sh
cargo build --locked --bin dxt
mkdir -p examples/csv_to_fixed/out
target/debug/dxt check examples/csv_to_fixed/job.yml --scan
target/debug/dxt explain examples/csv_to_fixed/job.yml
target/debug/dxt run examples/csv_to_fixed/job.yml --threads 4
cmp examples/csv_to_fixed/expected/records.dat examples/csv_to_fixed/out/records.dat
target/debug/dxt run examples/csv_to_fixed/readback.yml --threads 4
cmp examples/csv_to_fixed/expected/readback.csv examples/csv_to_fixed/out/readback.csv
```

相対パスは YAML のあるディレクトリを基準に解決します。出力には `mode: overwrite` を指定しているため、同じコマンドで再実行できます。`out/` は Git の管理対象外です。

中間データセットが不要なサンプルでは `process` を省略しています。省略した場合は `process: []` と同じ扱いになります。

固定長の NULL は、文字・日時では空白、数値では0、バイナリではゼロバイトへ補完されます。通常値との区別は失われます。`csv_to_fixed/readback.yml` は空白の文字・日時・真偽値に NULL 入力規則を指定しています。金額は0として読み戻すことを期待しています。

## Parquet と圧縮ファイルの例

Parquetの例は [parquet/job.yml](parquet/job.yml) → [parquet/readback.yml](parquet/readback.yml) の順に実行します。CSVをZstd圧縮のParquetへ保存し、型・NULL・空文字を保ったままCSVへ戻します。Parquetには `fields`・`encoding` を指定せず、圧縮設定は `parquet.compression` です。

圧縮ファイルの例は [compression/job.yml](compression/job.yml) → [compression/readback.yml](compression/readback.yml) の順です。付属のgzip入力を使い、CSVとCP932固定長それぞれのgzip・zstd出力を作ります。続いて4つの出力をすべて読み戻し、非圧縮のUTF-8 CSVを生成します。こちらは入力・出力の直下に `compression: gzip` / `compression: zstd` を指定します。

```sh
cargo build --locked --bin dxt
mkdir -p examples/parquet/out examples/compression/out
target/debug/dxt run examples/parquet/job.yml
target/debug/dxt run examples/parquet/readback.yml
cmp examples/parquet/expected/readback.csv examples/parquet/out/readback.csv
target/debug/dxt run examples/compression/job.yml
target/debug/dxt run examples/compression/readback.yml
cmp examples/compression/expected/csv.csv examples/compression/out/csv_gzip.csv
cmp examples/compression/expected/csv.csv examples/compression/out/csv_zstd.csv
cmp examples/compression/expected/fixed.csv examples/compression/out/fixed_gzip.csv
cmp examples/compression/expected/fixed.csv examples/compression/out/fixed_zstd.csv
```

## 全サンプルを検証する

```sh
cargo test --locked --test examples -- --nocapture
python3 examples/generate_fixtures.py --check
```

[tests/examples.rs](../tests/examples.rs) は、入力と設定を一時ディレクトリへコピーし、1スレッド・4スレッドの両方で全ジョブを検証します。各ジョブで `check`、`check --scan`、`explain`、`run` を実行し、`run` は同じ場所で2回繰り返します。通常の実行ディレクトリを YAML のある場所から分離し、`--temp-dir` を出力先と同じファイルシステムに置きます。

CSV・固定長の正常系は出力の全バイトを期待値と比較します。Parquetと圧縮ファイルはファイルの生成を確認したうえで、読み戻したCSVの全バイトを独立した期待値と比較します。Parquetの型・圧縮・行グループ等は `tests/parquet.rs`、ファイル全体のgzip/zstd圧縮・破損検出・連結フレーム等は `tests/compression.rs` で別途検証します。これにより、レコード長、末尾の幅埋め、NULL と空文字、改行コード、文字コード、数値や日時の値、行順まで検証します。`check` / `explain` が出力を作成しないこと、実行後に一時ファイルが残らないことも確認します。自動テストは実際の `examples/*/out/` を変更しません。

異常系では次の終了コードとエラーメッセージを照合し、出力が公開されないことを確認します。`width_overflow.yml` は最初の CSV 出力を作成した後、2つ目の固定長出力で失敗させ、両方とも公開されないことを検証します。

| ジョブ | `check --scan` | `run` |
| --- | --- | --- |
| [malformed_csv.yml](errors/malformed_csv.yml) | 3 | 3 |
| [short_fixed.yml](errors/short_fixed.yml) | 3 | 3 |
| [width_overflow.yml](errors/width_overflow.yml) | 0 | 5 |
| [precision_loss.yml](errors/precision_loss.yml) | 0 | 5 |
| [unrepresentable.yml](errors/unrepresentable.yml) | 0 | 5 |

`check --scan` は入力を走査しますが、出力値の変換は `run` で行います。上記の異常系も、通常の `check` と `explain` は成功する設定です。

期待ファイルは [generate_fixtures.py](generate_fixtures.py) に明示した文字列・既知のバイト列と Python 標準ライブラリで生成しています。`dxt` の出力を期待値に流用しません。入力・期待値・[manifest.json](manifest.json) を再生成するときは次を実行します。

```sh
python3 examples/generate_fixtures.py
```
