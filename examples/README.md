# 入出力の組み合わせサンプル

各YAMLの生成ドキュメントは同じフォルダの同名 `.md` にあります。[ドキュメント一覧](DOCUMENTS.md)から参照できます。ジョブは `dxt doc` の出力、import用の部品定義は内容と参照元を掲載しています。生成する見出し・表・定型文は英語で、YAMLの説明文や設定値は原文を保持します。

```sh
cargo build --locked --bin dxt
python3 examples/generate_docs.py
python3 examples/generate_docs.py --check
```

固定長・行順編成・CSV・RDW・Parquet の変換を、入力ファイル、実行用 YAML、期待ファイルとともに収録しています。正常系と異常系のジョブをまとめて検証できます。加えて、[SQLite の外部テーブル](sqlite/README.md)を LuaJIT のプラグインで読み込む例があります。

| ディレクトリ | 入力 → 出力 | 確認する内容 |
| --- | --- | --- |
| [lua_job](lua_job/README.md) | Lua → CSV / Lua | ジョブ共有の `dxt.job`、共有カウンター、任意の開始・終了処理 |
| [sqlite](sqlite/README.md) | SQLite + CSV → CSV / SQLite | LuaJIT FFI、共有ライブラリの検索 / 直接パス、JOIN・集計、追記・全件置換・UPSERT、64bit整数、NULL・空文字・BLOB |
| [csv_to_csv](csv_to_csv/job.yml) | CSV → CSV / TSV | 先頭ゼロ、NULL と空文字、引用符・カンマ・項目内改行、最終改行なし、Decimal、真偽値、日付、hex → Base64、列別引用指定 |
| [csv_to_fixed](csv_to_fixed/job.yml) | CSV → 固定長 → CSV | UTF-8 日本語のバイト幅、LF、明示 offset、FILLER、右寄せ、NULL 補完、読み戻し |
| [fixed_to_csv](fixed_to_csv/job.yml) | 固定長 → CSV / TSV | CP932 → UTF-8 / CP932、CRLF、末尾区切り省略、ASCII 符号付きゾーン10進数、正規表現による NULL 判定 |
| [fixed_to_fixed](fixed_to_fixed/job.yml) | 固定長 → 固定長 → CSV | CP932 → UTF-16LE / UTF-8、CRLF → LF / 区切りなし、全角空白による幅埋め、UTF-16 の改行・読み戻し |
| [lines](lines/job.yml) | 行順編成 → CSV / 行順編成 → CSV | LINE SEQUENTIAL、短い行の空白補完、空行、最終改行の省略、LF → CRLF、末尾空白の除去・保持、読み戻し |
| [rdw_framing](rdw_framing/job.yml) | RDW → RDW / BDW / spanned → CSV | bdw / spannedの4通り、VB / VBSの入出力、文字・パック10進数を含む論理レコード、文字の途中での分割・結合、読み戻し |
| [csv_dialects](csv_dialects/job.yml) | CSV → CSV → CSV | ヘッダーなし、セミコロン区切り、単一引用符、CP932、UTF-16LE TSV、ISO-2022-JP、BOM 付き UTF-16BE の入力 |
| [binary_fields](binary_fields/job.yml) | 固定長 → CSV / 固定長 | 全23型、符号・エンディアン、IEEE / IBM HFP（`hfloat-le` / `hfloat-be`）、EBCDIC / ASCII ゾーン、パック10進数、ビット順の変更、生バイト一致 |
| [bstring](bstring/job.yml) | EBCDIC 固定長 → 固定長 / RDW / CSV | 追加の24番目の型 bstring、元バイトの保持、EBCDIC のバイト順ソート、icu_decode / icu_encode による文字列との JOIN、読み戻し |
| [csv_to_binary](csv_to_binary/job.yml) | CSV → 固定長 → CSV | 全23型の固定長出力、SQL CAST、負数・ゼロ・暗黙小数点、読み戻し |
| [null_defaults](null_defaults/job.yml) | CSV → 固定長 | 全23型の NULL 補完バイト、NULL と true が混在するビット領域 |
| [datetime](datetime/job.yml) | CSV → 固定長 / CSV → CSV | 2桁年の1970〜2069年への復元、ナノ秒、UTC / Asia/Tokyo、タイムゾーンなし日時、`Z` の `+0900` 表記 |
| [join_aggregate](join_aggregate/job.yml) | 固定長 + CSV → CSV + 固定長 | フィルタ、JOIN、該当マスターなし、元の明細順、SUM / COUNT、明示ソート、複数出力 |
| [empty](empty/job.yml) | CSV / 固定長 → CSV / 固定長 | 0件入力、ヘッダーのみ、ヘッダーなし、0バイトの固定長出力 |
| [parquet](parquet/job.yml) | CSV → Parquet → CSV | スキーマ・Decimal・バイナリ・NULL・空文字・改行・元順の保持、Zstd圧縮、fields / encoding なしの読み戻し |
| [compression](compression/job.yml) | gzip CSV / 固定長 → gzip / zstd → CSV | 明示したcompression、UTF-8 / CP932、NULL・空文字・Decimal・バイナリ・項目内改行、4種類の圧縮出力の読み戻し |
| [conditional_records](conditional_records/job.yml) | 固定長 → CSV / 固定長 / RDW → CSV | SQL式の `when` によるヘッダー・明細・トレーラーの領域共有、非該当列のNULL、入出力の共通定義・読み戻し |
| [checks](checks/job.yml) | CSV → CSV / 異常終了 | 入出力の `check`、SQL条件、warn → syserr / info → sysout、入力・出力の error による異常終了 |
| [exit_codes](exit_codes/job.yml) | CSV → CSV / 異常終了 | exit_codesのimport、正常終了、入力エラー3→12・出力エラー5→20への置換 |
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

## bstring で文字コードを保持する

[bstring/fields.yml](bstring/fields.yml) の code は CP939 の4バイトの項目を Binary として読み、末尾のエンコード済み空白 `40` を除去します。payload は `trim: false` で2バイトをそのまま保持し、文字コードとして不正な値も含みます。[job.yml](bstring/job.yml) は入力のバイト列を固定長とRDWに戻し、code のバイト順でソートしたCSVと、UTF-8 のマスターを `icu_encode` して照合したCSVを作ります。ソート結果は `a` → `A` → `0` です。

```sh
mkdir -p examples/bstring/out
target/debug/dxt run examples/bstring/job.yml
cmp examples/bstring/expected/records.dat examples/bstring/out/records.dat
cmp examples/bstring/expected/sorted.csv examples/bstring/out/sorted.csv
cmp examples/bstring/expected/joined.csv examples/bstring/out/joined.csv
target/debug/dxt run examples/bstring/readback.yml
cmp examples/bstring/expected/readback.csv examples/bstring/out/fixed_readback.csv
cmp examples/bstring/expected/readback.csv examples/bstring/out/rdw_readback.csv
```

文字列へ変換する場合は `icu_decode(code, 'cp939')`、固定幅のバイト列へ変換する場合は `icu_encode(text, 'cp939', 4)` とします。長さは文字数ではなく、指定文字コード上のバイト数です。両方の関数とも末尾空白を保持します。`space_char` は1文字を指定し、bstring ではエンコードしたバイト列で trim・padding します。旧 `pad_char` は使用できません。

[fullwidth.yml](bstring/fullwidth.yml) は CP932・JEF・UTF-16LE / BE・CP930 / CP939・ISO-2022-JP の7種類で「　日本　」をバイナリへ変換し、全角空白で trim した後、全角空白を使って12バイトまで埋めます。出力には元バイト数、trim 後と padding 後の文字列、padding 後のバイト数を含めます。シフト制御を持つ文字コードでも、そのバイトを含めた長さが12になることを検証します。

```sh
target/debug/dxt run examples/bstring/fullwidth.yml
cmp examples/bstring/expected/fullwidth.csv examples/bstring/out/fullwidth.csv
```

## レコード条件で項目定義を切り替える

[conditional_records/fields.yml](conditional_records/fields.yml) は、先頭1バイトの `record_type` を共通項目として、同じ16バイトのレコードを3通りに解釈します。

| 種別 | `when` の例 | 残り15バイトの内容 |
| --- | --- | --- |
| H | `record_type = 'H'` | 日付8バイト + 送信元7バイト |
| D | `record_type IN ('D')` | 顧客ID7バイト + 金額8バイト |
| T | `record_type = 'T'` | 明細件数7バイト + 合計金額8バイト |

`when` は Boolean の SQL 式です。`AND` / `OR` / `IN` やスカラー関数を使えます。入力の条件式は `when` のない共通項目を参照し、出力では SQL の結果列を参照します。該当しない入力列は NULL になり、出力では該当する項目だけを書きます。条件付き項目の領域を共有するため、各種別の最初の項目で `offset: 1` に戻しています。同じ行で有効な項目の隙間・重複はエラーになります。

[job.yml](conditional_records/job.yml) は入力を CSV に展開し、同じ `fields.yml` を使って固定長とRDWにも書き出します。[readback.yml](conditional_records/readback.yml) はその両方をCSVに戻します。

```sh
cargo build --locked --bin dxt
mkdir -p examples/conditional_records/out
target/debug/dxt check examples/conditional_records/job.yml --scan
target/debug/dxt run examples/conditional_records/job.yml --threads 4
cmp examples/conditional_records/expected/records.csv examples/conditional_records/out/records.csv
cmp examples/conditional_records/expected/records.dat examples/conditional_records/out/records.dat
cmp examples/conditional_records/expected/records.rdw examples/conditional_records/out/records.rdw
target/debug/dxt run examples/conditional_records/readback.yml --threads 4
cmp examples/conditional_records/expected/records.csv examples/conditional_records/out/fixed_readback.csv
cmp examples/conditional_records/expected/records.csv examples/conditional_records/out/rdw_readback.csv
```

## check で異常終了・ログ出力する

[checks/job.yml](checks/job.yml) は入力の負の金額を `warn` で標準エラーへ、未指定の金額を `info` で標準出力へ記録し、処理を続けます。出力では SQL で金額を2倍した結果に対して `check` を実行します。メッセージは定義ID・レコード番号等を含む JSON ログです。`when` が FALSE / NULL のチェックは何もしません。

```sh
cargo build --locked --bin dxt
mkdir -p examples/checks/out
target/debug/dxt run examples/checks/job.yml --threads 4
cmp examples/checks/expected/records.csv examples/checks/out/records.csv
```

`check` は1件のマップ、または複数件の配列で指定できます。入力ではデコード後の全公開列を参照し、出力では SQL 結果の列を参照します。複数のチェックはレコードごとに記載順に評価します。

[input_abend.yml](checks/input_abend.yml) は負の入力金額で `error` を発生させて終了コード3になります。[output_abend.yml](checks/output_abend.yml) は変換後の負の金額で終了コード5になります。どちらもメッセージを標準エラーへ記録し、出力を公開しません。

```sh
target/debug/dxt run examples/checks/input_abend.yml
# 終了コード3。out/input_abend.csv は作成されない。
target/debug/dxt run examples/checks/output_abend.yml
# 終了コード5。out/output_abend.csv は作成されない。
```

通常の `check` / `explain` は条件式を検証するだけでメッセージを出しません。`check --scan` は入力のチェックを実行し、出力のチェックは `run` で実行します。`check` を持つ入出力はログと異常終了位置の順序を保つため、レコード順に変換・検査します。

## 集計チェック・業務パラメーター・CSV列名マッピング

[validation/job.yml](validation/job.yml) は日本語CSVヘッダーを `source_name` でSQL列名へ対応付け、4つのタイミングで件数・合計を検査します。`params` の `type` で文字列・数値・日時・真偽値を宣言できます。CLIのテキスト値、またはSQL式の `default` を宣言した型へ変換し、SQL内の `${名前}` にバインドします。`boolean` のCLI値は `true` / `false`（大文字・小文字を区別しない）です。

```sh
mkdir -p examples/validation/out
target/debug/dxt doc examples/validation/job.yml > /tmp/validation.md
target/debug/dxt check examples/validation/job.yml --scan
target/debug/dxt run examples/validation/job.yml
cmp examples/validation/expected/records.csv examples/validation/out/records_2026-11-06.csv
target/debug/dxt run examples/validation/job.yml -p region=west -p business_date=2026-11-07
```

必須にするパラメーターは `region: {type: string}` のように `default` を省略します。宣言がない名前、重複指定、必須値の未指定、型変換できない値は `check` / `explain` / `run` でエラーになります。日時パラメーターは `date` / `time` / `timestamp`、小数は `type: decimal` だけを指定し、精度と小数桁は値から自動で決まります。

パスには `${名前}` を指定できます。この例の入力は `data/${source_file}.csv`、出力は `out/records_${business_date}.csv` で、上の上書き実行は `out/records_2026-11-07.csv` を作成します。CLIからパス自体を指定する場合は `'--output.result.path=out/custom_${business_date}.csv'` のように引用します。相対パスは展開後に、YAMLの記述元またはCLIの実行ディレクトリを基準に解決します。

## 全サンプルを検証する

```sh
cargo test --locked --test examples --test lua_plugins --test lua_outputs --test lua_job --test lua_security -- --nocapture
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
| [input_abend.yml](checks/input_abend.yml) | 3 | 3 |
| [output_abend.yml](checks/output_abend.yml) | 0 | 5 |

`check --scan` は入力を走査しますが、出力値の変換は `run` で行います。上記の異常系も、通常の `check` と `explain` は成功する設定です。

ファイル変換の期待ファイルは [generate_fixtures.py](generate_fixtures.py) に明示した文字列・既知のバイト列と Python 標準ライブラリで生成しています。SQLite の期待 CSV は [sqlite/expected](sqlite/expected) に明示しています。`dxt` の出力を期待値に流用しません。ファイル変換の入力・期待値・[manifest.json](manifest.json) を再生成するときは次を実行します。

```sh
python3 examples/generate_fixtures.py
```
