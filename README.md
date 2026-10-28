# dxtran

`dxt` は、YAML に定義した CSV・固定長・RDW・Parquet ファイルを Apache DataFusion SQL で変換する Rust 製 CLI です。設計仕様は [docs/SPEC.md](docs/SPEC.md) にあります。

```sh
cargo build --locked --release
cargo run -- check examples/basic/job.yml --scan
cargo run -- explain examples/basic/job.yml
cargo run -- run examples/basic/job.yml --threads 4 --memory-limit 512MiB
```

サンプルの結果は `examples/basic/totals.csv` に出力されます。相対パスは、その値を記述した YAML ファイルがあるディレクトリから解決します。既存出力を置き換えるには出力定義に `mode: overwrite` を指定します。

入力・出力の `path` は YAML では省略でき、`check`・`explain`・`run` の引数で `--input.<id>.path` / `--output.<id>.path` として指定できます。引数は YAML の値より優先し、引数の相対パスはコマンド実行時のカレントディレクトリから解決します。各ファイルのパスは YAML または引数のいずれかで必ず指定してください。存在しない ID、空のパス、同じ引数の重複指定は設定エラーです。`=` で値を結ぶ形式と、空白で区切る形式の両方を使えます。

```sh
dxt run job.yml --input.sales.path=sales.csv --output.totals.path=totals.csv
```

固定長・CSV の各組み合わせ、文字コード、全23型、読み戻し、異常系のサンプルと一括検証手順は [examples/README.md](examples/README.md) を参照してください。

`format: lua` で LuaJIT のプラグインを外部テーブルとして定義し、データベース等のデータを SQL から参照できます。出力では `mode: append`（追記）・`overwrite`（全件置換）・`merge`（キーによる UPSERT）を指定できます。共有ライブラリは `LD_LIBRARY_PATH` 等による検索と直接パス指定に対応します。[外部テーブル API](docs/LUA_PLUGINS.md) と、[SQLite の JOIN・集計・書き込みサンプル](examples/sqlite/README.md)を用意しています。

Lua は既定で無効です。信頼できるコードに限り、`dxt run --allow-lua job.yml` のように実行者が明示許可してください。`check` / `explain` でも許可するとトップレベルを評価します。FFI は実行プロセスと同じ権限を持ち、サンドボックスではありません。[リスク・対策・隔離の設計](docs/LUA_SECURITY.md)を参照してください。

ジョブ直下の `lua.variables` で初期値を定義すると、各 Lua プラグインから `dxt.job` を通してジョブ全体の変数を共有できます。カウンターや設定値のほか、必要なら FFI のハンドル等も保持できます。任意の `lua.path` で開始・終了処理を指定できます。[共有変数のサンプル](examples/lua_job/README.md)を参照してください。

SQL では Unicode 日時パターンを受け取る `icu_to_char`・`icu_to_date`・`icu_to_timestamp`・`icu_to_time` を使えます。第3引数のロケールは既定 `en-US` で、`ja-JP-u-ca-japanese` により ICU の和暦を選べます。日付の戻り値は Date32、時刻は Time64(us)、タイムスタンプは Timestamp(us, UTC) です。[引数・対応範囲](docs/SPEC.md#101-icu-日時関数)を参照してください。

```sql
SELECT icu_to_char(DATE '2026-10-28', 'GGGGy年M月d日', 'ja-JP-u-ca-japanese');
-- 令和8年10月28日
SELECT icu_to_date('R08/10/28', 'GGGGGyy/MM/dd', 'ja-JP-u-ca-japanese');
-- 2026-10-28
SELECT icu_to_timestamp('令和元年5月1日 00:00', 'GGGGy年M月d日 HH:mm',
                        'ja-JP-u-ca-japanese', 'Asia/Tokyo');
-- 2019-04-30T15:00:00Z
SELECT icu_to_time('13:02:03', 'HH:mm:ss');
-- 13:02:03
```

CSV・fixed・RDW の入力定義は全列の名前・型を指定します。CSV の未引用の空フィールドは NULL、引用した空フィールドは空文字です。`output.fields` の部分指定では SQL の列順を保ったまま書式を設定できます。fixed / RDW は全物理項目を定義し、隙間は名前のない FILLER で明示します。23種類の型、文字コード、ビット列、日時パターン、NULL 表現を共通のレイアウトで処理します。

文字項目の `input.fields` / `output.fields` には `trim: false | left | right | both` を指定できます。`false` は保持、`left` は先頭、`right` は末尾、`both` は両端の空白を除去します。fixed / RDW で `pad_char` を指定した場合はその文字が対象です。固定幅出力では trim 後に項目幅まで再度埋めます。例えば長さ5・`trim: both` の項目に `"  A  "` を出力すると `"A    "` になります。[省略時の動作と詳細](docs/SPEC.md#74-文字項目日付時刻binarybits)を参照してください。

`encoding` は従来の UTF-8・CP932 等に加え、[jtc-utils](https://docs.rs/jtc-utils/0.1.0/jtc_utils/charset/) の `jef`、`x-fujitsu-ebcdic-ascii`、`x-fujitsu-ebcdic-kana`、`x-fujitsu-ebcdic-lower`、`cp930`、`cp939`、`cp942`、`cp942c`、`cp943c` に対応します。例えば IBM の漢字混在データは `encoding: cp939` と指定します。JEF は2バイトの漢字コード専用で、固定長・RDW の文字項目の埋め文字には `pad_char: "　"` を指定してください。CP930 / CP939 の文字項目は SO/SI を含む独立した文字列として扱い、`length` はシフト制御バイトを含むバイト数です。不正なバイト列と変換不能文字はエラーになります。[文字コードの詳細](docs/SPEC.md#65-encoding-の解決と文字コード変換)を参照してください。

fixed / RDW の input/output では、項目に `when: "record_type = 'H'"` のような Boolean の SQL 式を指定し、レコード条件に応じて同じバイト領域を別の項目として扱えます。入力の条件式は `when` のない共通項目、出力の条件式は SQL の結果列を参照します。条件が TRUE の項目だけを読み書きし、非該当の入力列は NULL になります。`AND` / `OR` / `IN` / `IS NULL` やスカラー関数も利用できます。[ヘッダー・明細・トレーラーのサンプル](examples/conditional_records/job.yml)と[詳しい規則](docs/SPEC.md#711-レコード条件付き項目定義)を参照してください。

すべての形式の input/output に `check` を指定できます。`when` の SQL 式が TRUE のレコードに対し、`error` はメッセージを標準エラーへ出して異常終了、`warn` は標準エラーへ出して継続、`info` は標準出力へ出して継続します。入力ではデコード後の全公開列、出力では SQL 結果の列を参照できます。複数のチェックは配列で指定し、記載順に実行します。メッセージは文字列のまま JSON ログに記録します。[継続・異常終了のサンプル](examples/checks/job.yml)を用意しています。

```yaml
check:
  - when: "amount < 0"
    level: error
    message: "金額が負のため異常終了します。"
```

CSV・固定長・RDW は、入力・出力それぞれの直下に `compression: gzip` または `compression: zstd` を指定すると、ファイル全体をストリームで展開・圧縮します。省略時は非圧縮で、拡張子による自動判定は行いません。入力の展開後に文字コード変換・レコード解析を行い、出力は文字コード変換・レコード組み立て後に圧縮します。[圧縮形式の相互変換と読み戻しのサンプル](examples/compression/job.yml)を用意しています。

```yaml
input:
  - id: data
    format: csv
    path: input.csv.gz
    compression: gzip
    encoding: utf-8
    fields: [{name: value}]
output:
  - id: result
    format: csv
    path: output.csv.zst
    compression: zstd
    encoding: utf-8
    sql: SELECT * FROM data
```

Parquet は入力ファイルのスキーマとSQL結果の型を使用し、`fields`・`encoding` を指定しません。文字列・数値・日時・バイナリ・NULLに加え、リストや構造体もArrowのまま受け渡します。列名は他の形式と同じ `[a-z][a-z0-9_]*` の規則です。

```yaml
input:
  - {id: data, format: parquet, path: input.parquet}
output:
  - id: result
    format: parquet
    path: output.parquet
    sql: SELECT * FROM data
    parquet: {compression: zstd, row_group_size: 8192}
```

`parquet` は出力専用の省略可能な設定です。`compression` は `snappy`（既定）・`uncompressed`・`zstd`・`gzip`・`brotli`・`lz4_raw`、`row_group_size` は行グループの最大行数（既定8,192）です。Parquetではデータセット直下の `compression` は指定せず、出力の `parquet.compression` を使います。入力の圧縮方式はファイルから読み取ります。スキーマを持つ0行ファイルも扱えます。CSV・固定長・RDWとの相互変換やJOINも同じSQLで実行でき、[CSV → Parquet → CSVのサンプル](examples/parquet/job.yml)を用意しています。

`import` はマップ・配列・スカラーの位置で利用でき、マップは利用側を優先して再帰マージし、配列は置換します。`process` は不要なら省略でき、`process: []` と同じ扱いになります。処理 SQL は前方参照できる名前付きビューです。元ファイル順を内部キーで伝搬し、JOIN、集約、DISTINCT、集合演算、明示ソートと LIMIT の順序を確定します。順序伝搬を実装していない演算は計画時にエラーになります。

通常の入出力は有界バッファで逐次処理し、DataFusion のメモリプールと spill を使用します。`--memory-limit` は DataFusion 管理下の上限で、プロセス全体のメモリ上限ではありません。全出力の書き込み・同期が完了した後に公開します。`--temp-dir` は出力先と同一ファイルシステム・同一マウントに置いてください。既定は OS の一時ディレクトリです。

作業領域は `tempfile` で管理します。Linux では作業領域と spill を `0700`、一時ファイルと新規出力を `0600` で作成します。Windows では一時フォルダーのアクセス権を継承します。`--temp-dir` を指定する場合は、そのフォルダーに適切なアクセス権を設定してください。上書き時には既存出力の所有者とアクセス権を公開前に引き継ぎ、引き継げなければ全出力の公開を中止します。Linux では mode / 所有者 / グループ / POSIX ACL、Windows では所有者 / グループ / DACL / 整合性ラベルを扱います。Windows の継承 ACE は明示 ACE として保持し、その後の親からの継承を無効にします。詳細と制限は [公開仕様](docs/SPEC.md#122-公開の保証と限界) を参照してください。

固定長ファイルの単純READ/WRITEとの差と、CSV・Parquetの測定結果は [ベンチマーク](docs/benchmark.md) にまとめています。

非圧縮の固定長入力のデコードと固定長出力のエンコードは `--threads` に応じて並列実行し、結果は元の行順で受け渡し・書き込みします。バッチ数・サイズを制限し、メモリ設定が小さい場合は同時実行数も抑えます。単純な UTF-8 文字列1項目（NULL 判定の正規表現なし）や binary 1項目、1スレッド指定では直列変換を使い、並列化の追加コストを避けます。展開後サイズの上限推定が大きい入力も、実サイズを確認する従来の経路を使います。

圧縮された固定長入力は先頭から逐次展開してレコードを読みます。固定長出力の並列エンコードは圧縮時も利用し、順番を揃えたバイト列を1つの圧縮ストリームへ渡します。gzipの連結メンバー・zstdの連結フレームにも対応します。全入力のチェックサム・末尾まで検査するには `check --scan` を使ってください。

CSV は文字コード変換と引用状態を保つ字句解析を逐次実行し、解析済みのバッチの型変換と、出力バッチのCSV整形を `--threads` に応じて並列実行します。出力の文字コード変換・圧縮・書き込みは元順に行い、ISO-2022-JP等の状態と圧縮バイト列の再現性を保ちます。軽いUTF-8文字列1項目などは直列処理を使います。

Parquet入力は行グループを並列デコードし、ファイルの行順に返します。グループ全体をArrow配列として保持せず、バッチ単位で受け渡します。グループ境界の端数をまとめ、直列時と同じバッチ境界を維持します。並列変換は共有の上限と有界チャネルで制限し、1スレッド指定や小さいメモリ設定では直列経路を使います。Parquet出力は逐次処理です。[Parquet・CSV並列化の測定結果](benchmarks/PARALLEL_FORMATS.md)と[以前のCSV高速化](benchmarks/CSV.md)を参照してください。

式サブクエリ（スカラー、`EXISTS`、`IN` / `NOT IN`）、`DISTINCT ON`、`UNNEST` に対応します。スカラーサブクエリは 0 行なら NULL、複数行ならエラーです。`IN` は NULL による UNKNOWN と空集合を区別します。`DISTINCT ON` は指定された `ORDER BY`、同順位なら元の行順で代表行を選びます。`ORDER BY` がない場合は最初に現れた行を採用します。`UNNEST` は元の行順と配列の要素順を保ち、複数配列・入れ子の配列にも対応します。空配列と NULL 配列は 0 行、配列内の NULL は NULL の行になります。

相関集約や相関 `EXISTS` / `IN` は利用できますが、相関サブクエリ内の `LIMIT` / `OFFSET` や、`HAVING` で行を除く相関スカラー集約を `EXISTS` / `IN` で調べる形式など、DataFusion の非相関化で処理できない組み合わせは計画時エラーになります。公開処理は Linux（`renameat2`）と Windows のローカル NTFS（ハンドルによる rename）に対応します。Windows のネットワーク共有・FAT / exFAT は対象外です。

実行ログは標準エラーへの JSON Lines、`check` / `explain` の結果は標準出力です。終了コードは成功 `0`、設定 `2`、入力 `3`、SQL・リソース `4`、出力・公開 `5` です。

`dxt$` 始まりの識別子は内部処理用の予約名です。ユーザーSQLの列名・別名・CTE名などに使うと、引用符や大文字・小文字にかかわらず計画時エラー（終了コード `4`）になります。文字列値やコメントに含めることはできます。

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

固定長処理の[並列化の比較](benchmarks/PARALLEL_FIXED.md)、[Arrow 直接変換の比較](benchmarks/DIRECT_ARROW.md)、[最初の高速化前後の比較](benchmarks/OPTIMIZATION.md)、単純 READ/WRITE との[初回比較結果](benchmarks/RESULTS.md)、[再現手順](benchmarks/README.md)も参照できます。
