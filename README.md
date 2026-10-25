# dxtran

`dxt` は、YAML に定義した CSV・固定長・RDW ファイルを Apache DataFusion SQL で変換する Rust 製 CLI です。設計仕様は [docs/SPEC.md](docs/SPEC.md) にあります。

```sh
cargo build --locked --release
cargo run -- check examples/basic/job.yml --scan
cargo run -- explain examples/basic/job.yml
cargo run -- run examples/basic/job.yml --threads 4 --memory-limit 512MiB
```

サンプルの結果は `examples/basic/totals.csv` に出力されます。相対パスは、その値を記述した YAML ファイルがあるディレクトリから解決します。既存出力を置き換えるには出力定義に `mode: overwrite` を指定します。

入力定義は全列の名前・型を指定します。CSV の未引用の空フィールドは NULL、引用した空フィールドは空文字です。`output.fields` の部分指定では SQL の列順を保ったまま書式を設定できます。fixed / RDW は全物理項目を定義し、隙間は名前のない FILLER で明示します。23種類の型、文字コード、ビット列、日時パターン、NULL 表現を共通のレイアウトで処理します。

`import` はマップ・配列・スカラーの位置で利用でき、マップは利用側を優先して再帰マージし、配列は置換します。処理 SQL は前方参照できる名前付きビューです。元ファイル順を内部キーで伝搬し、JOIN、集約、DISTINCT、集合演算、明示ソートと LIMIT の順序を確定します。順序伝搬を実装していない演算は計画時にエラーになります。

通常の入出力は有界バッファで逐次処理し、DataFusion のメモリプールと spill を使用します。`--memory-limit` は DataFusion 管理下の上限で、プロセス全体のメモリ上限ではありません。全出力の書き込み・同期が完了した後に公開します。`--temp-dir` は出力先と同一ファイルシステム・同一マウントに置いてください。既定は OS の一時ディレクトリです。

現在の原子的な公開処理は Linux の `renameat2` に対応します。SQL 内の式サブクエリ、DISTINCT ON、UNNEST 等は順序伝搬の未対応演算として計画時に拒否します。

実行ログは標準エラーへの JSON Lines、`check` / `explain` の結果は標準出力です。終了コードは成功 `0`、設定 `2`、入力 `3`、SQL・リソース `4`、出力・公開 `5` です。

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

固定長処理の[追加高速化の比較](benchmarks/DIRECT_ARROW.md)、[最初の高速化前後の比較](benchmarks/OPTIMIZATION.md)、単純 READ/WRITE との[初回比較結果](benchmarks/RESULTS.md)、[再現手順](benchmarks/README.md)も参照できます。
