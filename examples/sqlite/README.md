# SQLite の読み取りと追記・全件置換・マージ

Lua は既定で無効です。このサンプルは FFI で SQLite をロードするため、内容とライブラリを信頼できる場合だけ `--allow-lua` を指定します。`check` でもトップレベルを評価します。[セキュリティ境界](../../docs/LUA_SECURITY.md)を参照してください。

[sqlite.lua](sqlite.lua) は LuaJIT FFI から SQLite の C API を呼び出すプラグインです。[job.yml](job.yml) の `customers` を SQLite、`sales` を CSV から読み込み、JOIN した明細、顧客一覧、地域ごとの集計を CSV に出力します。元の売上順、該当する顧客がない明細、64bit整数の上限・下限、NULL と空文字、BLOB を確認できます。

Rust と C のビルド環境、Python 3（サンプル DB 作成用）、SQLite 3 の共有ライブラリを使用します。Python は DB 作成時だけ必要です。dxt の実行時には SQLite の CLI や Lua 用 SQLite モジュールは必要ありません。

```sh
cargo build --locked --bin dxt
python3 examples/sqlite/create_db.py
mkdir -p examples/sqlite/out
target/debug/dxt check --allow-lua examples/sqlite/job.yml
target/debug/dxt check --allow-lua examples/sqlite/job.yml --scan
target/debug/dxt explain --allow-lua examples/sqlite/job.yml
target/debug/dxt run --allow-lua examples/sqlite/job.yml --threads 4
cmp examples/sqlite/expected/details.csv examples/sqlite/out/details.csv
cmp examples/sqlite/expected/customers.csv examples/sqlite/out/customers.csv
cmp examples/sqlite/expected/totals.csv examples/sqlite/out/totals.csv
```

`create_db.py` はサンプル専用の `data/customers.sqlite` を再作成します。入力 DB は読み取り専用で開き、クエリも単一の読み取り専用文であることを検査します。整数は SQLite から10進文字列として取り出すため、LuaJIT の浮動小数点を経由せずに `int64` へ変換されます。TEXT / BLOB はバイト長を指定して取り出し、NUL を含む値と空値を保持します。

共有ライブラリの設定は次の部分です。

```yaml
lua:
  libraries:
    sqlite3: libsqlite3.so.0
```

Linux で別の配置場所から検索するには、dxt を起動するときに指定します。

```sh
LD_LIBRARY_PATH=/opt/sqlite/lib target/debug/dxt run --allow-lua examples/sqlite/job.yml
```

直接パスを指定する場合は `sqlite3` の値を変更します。相対パスはその値を記述した YAML の場所が基準です。

```yaml
lua:
  libraries:
    sqlite3: /opt/sqlite/lib/libsqlite3.so.0
    # sqlite3: ./native/libsqlite3.so.0
    # Windows: sqlite3: 'C:\sqlite\sqlite3.dll'
    # macOS:   sqlite3: /opt/homebrew/opt/sqlite/lib/libsqlite3.dylib
```

Windows / macOS では、使用するライブラリ名またはパスに置き換えてください。LuaJIT とライブラリは同じ CPU アーキテクチャのものを使います。

他の SQLite テーブルを読むときは `lua.options.database`・`lua.options.query`・`fields` を変更します。クエリの結果列名と `fields` の名前を合わせ、安定した読み取り順が必要ならクエリに `ORDER BY` を付けます。[外部テーブル API の詳細](../../docs/LUA_PLUGINS.md) を参照してください。

## CSV を SQLite のテーブルへ反映する

[write.yml](write.yml) は [customer_updates.csv](data/customer_updates.csv) を3つのテーブルに反映します。`create_db.py` が DB と必要なテーブルを準備します。

| 出力 | モード | 結果 |
| --- | --- | --- |
| `customer_append` | `append` | 新規顧客 `0004` を挿入。既存の3件を保持 |
| `customer_overwrite` | `overwrite` | 全件置換し、更新 CSV の `0002`・`0004` だけを保存 |
| `customer_merge` | `merge` | `customer_id` をキーに `0002` を更新、`0004` を挿入。`0001`・`0003` を保持 |

```sh
python3 examples/sqlite/create_db.py
mkdir -p examples/sqlite/out
target/debug/dxt check --allow-lua examples/sqlite/write.yml
target/debug/dxt run --allow-lua examples/sqlite/write.yml --threads 4
target/debug/dxt run --allow-lua examples/sqlite/readback_writes.yml --threads 4
cmp examples/sqlite/expected/appended.csv examples/sqlite/out/appended.csv
cmp examples/sqlite/expected/overwritten.csv examples/sqlite/out/overwritten.csv
cmp examples/sqlite/expected/merged.csv examples/sqlite/out/merged.csv
```

追記の再実行では主キーが重複するため、最初から試す場合は `create_db.py` から実行します。`overwrite` と `merge` は同じ入力で繰り返せます。`overwrite` はテーブルの定義・制約・インデックスを保持し、DELETE と INSERT を同じトランザクションで行います。入力0件なら空のテーブルになります。

出力設定の例は次のとおりです。出力では SQL 結果からスキーマを得るため `fields`・`encoding` は指定しません。`path` は書き込み先 DB ではなくプラグインのファイル名です。

```yaml
output:
  - id: saved
    format: lua
    path: sqlite.lua
    mode: merge
    sql: SELECT customer_id, customer_name, region FROM updates
    lua:
      libraries: {sqlite3: libsqlite3.so.0}
      options:
        database: data/customers.sqlite
        table: customer_merge
        keys: [customer_id]
```

`keys` は既存の主キー・ユニークキーに合わせます。複合キーも指定でき、SQL 結果に含まれる必要があります。DB とテーブルの自動作成は行いません。複数出力は記載順に個別にコミットします。後続の出力が失敗した場合も、コミット済みの出力は保持されます。各出力の途中エラーは、その出力の変更を全件ロールバックします。

```sh
cargo test --locked --test lua_plugins --test lua_outputs
```

自動テストは一時ディレクトリで実行します。Linux ではこのサンプルの全コマンドと出力バイト列を1・4スレッドで確認し、`LD_LIBRARY_PATH` による検索、絶対パス、相対パスからのロードを実際に検証します。SQLite を使わないプラグイン基盤のテストは全プラットフォームで実行します。
