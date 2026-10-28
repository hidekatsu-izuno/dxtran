# LuaJIT による外部テーブル

Lua は既定で無効です。信頼できるコードに限り、CLI に `--allow-lua` を指定してください。これは `check` / `explain` / `check --scan` にも適用します。許可時はファイル・プロセス・ネットワーク・FFI を実行ユーザーの権限で利用でき、サンドボックスは設けません。YAML から自己許可することはできません。[セキュリティ境界と隔離の設計](LUA_SECURITY.md)を参照してください。

入力に `format: lua` を指定すると、LuaJIT のスクリプトを SQL のテーブルとして使えます。CSV・固定長・RDW・Parquet との JOIN、集計、中間ビュー、入力の `check`、既存の出力形式をそのまま利用できます。LuaJIT はバイナリに組み込みます。ビルドには Rust に加えて C のビルド環境が必要です。別途 `luajit` コマンドをインストールする必要はありません。

```yaml
input:
  - id: customers
    format: lua
    path: plugins/customers.lua
    lua:
      libraries:
        sqlite3: libsqlite3.so.0
      options:
        database: data/customers.sqlite
        query: SELECT customer_id, customer_name FROM customers ORDER BY customer_id
    fields:
      - {name: customer_id}
      - {name: customer_name, nullable: true}
output:
  - id: result
    format: csv
    path: out/customers.csv
    encoding: utf-8
    sql: SELECT * FROM customers
```

`path` はスクリプトのファイル名で、他の入力と同様に YAML の記述元を基準に解決します。CLI の `--input.customers.path` でも置き換えられます。`lua` は省略可能で、`libraries` と `options` は既定で空のマップです。`options` の値には文字列・数値・真偽値・配列・マップを使用できます。`encoding`、`compression`、他の形式の設定ブロックは指定しません。出力でも `format: lua` を指定できます。

## スクリプトの API

スクリプトは `open(options)` 関数を持つテーブルを返します。`open` は走査ごとに呼び出され、`next()` と任意の `close()` を持つカーソルを返します。これらは引数なしの関数として呼び出します。Lua の `:` 構文によるメソッドではありません。

```lua
return {
    open = function(options)
        local row = 0
        return {
            next = function()
                row = row + 1
                if row > 2 then return nil end
                return {customer_id = "000" .. row, customer_name = "顧客" .. row}
            end,
            close = function()
                -- DB 接続やカーソルを解放する。
            end,
        }
    end,
}
```

`next()` は列名をキーにした1行のテーブル、終了時は `nil` を返します。行の配列や `false` は終了として扱いません。`fields` にないキー、不正な型、非 nullable 列の `nil` は入力エラーです。文字列の trim や暗黙の文字列化は行いません。

| `fields.type` | Arrow / SQL 型 | Lua から返す値 |
| --- | --- | --- |
| `string`（省略時） | Utf8 | UTF-8 の文字列 |
| `int64` | Int64 / BIGINT | 10進整数の文字列、または安全な範囲の整数値 |
| `uint64` | UInt64 / BIGINT UNSIGNED | 非負の10進整数の文字列、または安全な範囲の整数値 |
| `float64` | Float64 / DOUBLE | 有限の数値 |
| `boolean` | Boolean | `true` / `false` |
| `binary` | Binary | 生のバイト列を保持する Lua 文字列 |

`fields` が受け付けるキーは `name`・`type`・`nullable` です。列名の規則は `[a-z][a-z0-9_]*`、重複は禁止です。`nullable` の既定値は `false`。`nil` またはキーの省略が SQL の NULL、`""` が空文字・空 BLOB となります。Decimal・日付・時刻等は文字列として返し、必要な SQL 型へ `CAST` してください。

LuaJIT の数値は倍精度浮動小数点なので、整数値として受け付ける範囲は ±9,007,199,254,740,991 までです。それより大きい整数は文字列を返してください。`"9223372036854775807"` や `"18446744073709551615"` を精度を失わずに読み取れます。FFI の cdata はスクリプト側で文字列または数値に変換してください。

## 共有ライブラリとパス

`dxt.load_library(name)` は `lua.libraries` の該当する値を [LuaJIT の `ffi.load`](https://luajit.org/ext_ffi_api.html#ffi_load) に渡し、ライブラリを返します。例えば `dxt.load_library("sqlite3")` は次のいずれの設定でも使えます。

```yaml
lua:
  libraries:
    sqlite3: libsqlite3.so.0  # OS のローダーで検索
```

Linux では、起動時の `LD_LIBRARY_PATH` を含む OS のライブラリ検索規則が適用されます。

```sh
LD_LIBRARY_PATH=/opt/sqlite/lib target/debug/dxt run --allow-lua examples/sqlite/job.yml
```

絶対パス、相対パスの指定も可能です。`/` または `\` を含む値は、その値を記述した YAML ファイルの場所から解決します。裸のライブラリ名はそのままローダーに渡します。

```yaml
lua:
  libraries:
    sqlite3: /opt/sqlite/lib/libsqlite3.so.0
    # sqlite3: ./native/libsqlite3.so.0
    # sqlite3: 'C:\sqlite\sqlite3.dll'
    # sqlite3: /opt/homebrew/opt/sqlite/lib/libsqlite3.dylib
```

`dxt.resolve_path(path)` は `lua.options` ブロックを記述した YAML の場所を基準にパスを解決します。`options` がない場合は `lua` ブロック、`lua` もない場合はスクリプトの場所が基準です。プラグインは `dxt.resolve_path(options.database)` のように使用できます。options の文字列自体は変換せず、URL・接続文字列にも使用できます。Lua の補助モジュールはスクリプトのあるディレクトリから `require` できます。

## 実行と後始末

通常の `check` / `explain` はスクリプトを評価し、戻り値と `open` の存在を検証します。`open` / `next` は呼び出しません。トップレベルには関数・FFI の型定義等を置き、接続・ライブラリのロードは `open` で行ってください。`check --scan` と `run` は実際に走査します。

ジョブ直下の `lua` を省略した場合、各走査は独立した LuaJIT VM とカーソルを持ちます。1つのカーソルは単一の読み取りスレッドで呼び出します。JOIN の両側、複数出力、再走査では別々の接続が開くことがあります。ジョブ共有の値が必要な場合は次節の `lua.variables` を使用します。プラグインは繰り返し読み取り可能にし、同じデータと順序が必要な場合は DB 側でも一貫したデータと `ORDER BY` を用意してください。外部データのスナップショットを dxt が固定することはありません。`next` が返した順序を既存の内部順序キーで SQL に引き継ぎます。SQL の射影・フィルタ・LIMIT の DB 側への自動プッシュダウンはありません。

EOF、読み取り・型変換・入力 check のエラー、ストリームの破棄時に `close` を1回呼びます。正常終了時の `close` エラーは入力エラーになります。元のエラーや途中破棄に伴う `close` エラーは元の処理を優先します。`open` がカーソルを返す前に失敗した場合の解放はプラグイン側で行ってください。OS による強制終了時は `close` の実行を保証しません。

行とバッチのデコードサイズは既存入力と同じ16 MiB以内に抑え、バッチの最大行数は8,192です。入力 `check` は行ごとに読み取り直後に実行します。LuaJIT のヒープや外部ライブラリの確保領域は DataFusion の `--memory-limit` の管理外です。

スクリプトと FFI は、dxt と同じ権限でファイル・ネットワーク・ネイティブコードへアクセスする信頼済みプラグインとして実行します。Lua のサンドボックスは設けません。YAML の options や行データを Lua ソースとして評価することはありません。

SQLite の実装と実行手順は [examples/sqlite](../examples/sqlite/README.md) を参照してください。

## ジョブ共有の変数

ジョブ直下に `lua.variables` を定義すると、すべての Lua 入出力から同じ `dxt.job` テーブルを読み書きできます。変数名・用途はプラグイン側で自由に決められます。DB 接続専用の設定やホスト側で解釈する変数名はありません。

```yaml
lua:
  variables:
    processed: 0
    label: daily
    settings: {enabled: true}
input:
  # 通常どおり入出力を定義する。
```

```lua
dxt.job.processed = dxt.job.processed + 1
local label = dxt.job.label
```

`variables` は任意の文字列キーを持つマップで、値は文字列・数値・真偽値・配列・マップを使用できます。実行中は通常の Lua テーブルなので、関数・Lua テーブル・userdata・FFI の cdata 等も格納できます。`lua: {}` でも空の共有テーブルを作れます。ジョブ直下の `lua` を省略すると `dxt.job` はなく、従来の独立した VM で実行します。各入出力に共有先の指定を追加する必要はありません。`run` ごとに初期値から作り直し、別ジョブへ値を持ち越しません。

### 任意の初期化・終了処理

開始・終了時に処理を行う場合だけ `lua.path` にフック用スクリプトを指定します。`lua.options` はフックへ渡す任意の設定です。

```yaml
lua:
  variables: {processed: 0}
  path: job.lua
  options: {label: daily}
```

```lua
-- job.lua
return {
    init = function(job, options)
        job.label = options.label
        -- 必要なら job.handle 等にリソースを格納する。
    end,
    close = function(job, success)
        -- カーソル・writer の終了後に、ジョブ共有リソースを解放する。
        -- success は、終了処理に入るまで成功していた場合に true。
    end,
}
```

`init(job, options)` / `close(job, success)` は両方とも省略可能です。通常の関数として呼び出し、`self` は渡しません。渡される `job` は各プラグインの `dxt.job` と同じテーブルです。

`run` と `check --scan` は計画・設定検証後、データ処理の前に `init` を1回呼びます。終了時には入力カーソルと出力 writer を先に閉じ、最後に `close` を1回呼びます。異常終了・SIGINT / SIGTERM の場合は `success=false` で呼びます。`init` が途中で失敗した場合も、設定済みの値を `close` に渡して解放を試みます。`close` 自体の失敗は終了コード5として報告し、元のエラーがある場合は元の終了コードとメッセージを保持して終了処理のエラーを付記します。OS による強制終了や、戻らないネイティブ呼び出しについては解放を保証しません。

通常の `check` / `explain` はフックを評価して関数の型を検証するだけで、`init` / `close` は呼びません。トップレベルには関数定義等を置き、変数更新やリソース確保は実行時の関数内で行ってください。`check --scan` は共有状態を初期化・終了するため、フックが行うログ出力等の副作用も発生します。出力 writer は開きません。

ジョブの `lua.libraries` はフックと全 Lua プラグインから `dxt.load_library(name)` で利用でき、入出力側の同名指定を優先します。共有ライブラリ名の検索・直接パスの規則は従来と同じです。`lua.path` は記述元の YAML、フック内の `dxt.resolve_path()` はジョブの `lua.options` ブロックの記述元（省略時は `lua` ブロック）を基準にします。入出力側のパス解決は各入出力の設定を使います。

共有状態を有効にした場合、同じジョブの Lua プラグインは同じ VM を使い、行の読み取り・デコード、書き込み、終了処理を直列化します。複数スレッドから同じリソースへ同時にアクセスせず、FFI の cdata を VM 間でコピーすることもありません。各スクリプトのグローバル環境と相対パス用ヘルパーは分離し、同じファイル・パス設定のモジュールは再利用します。共有したい値は `dxt.job` のフィールドへ格納してください。共有値を更新する処理の順序は SQL の実行順に従い、異なる入力間の更新順は固定しません。

共有変数の変更はメモリ上の通常の変更で、自動ロールバックやジョブ全体のトランザクションにはなりません。接続を保持する場合も、接続の作成・トランザクションの境界・解放は Lua 側で定義します。既存の出力ごとの `commit` / `rollback` は従来どおりです。

[共有カウンターと終了時の集計サンプル](../examples/lua_job/README.md)を参照してください。

## 出力テーブルへの追記・全件置換・マージ

出力の `path` もプラグインのスクリプトです。DB の保存先等は `lua.options` に指定します。出力の列名・型・順序は SQL 結果から取得し、`fields` は指定しません。出力用スクリプトを CLI で変更する場合は `--output.<id>.path` を使用します。

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
        table: customers
        keys: [customer_id]
```

| `mode` | SQLite サンプルの動作 |
| --- | --- |
| `append`（出力 Lua の既定） | INSERT で追記。主キー等の衝突はエラー |
| `overwrite` | DELETE で既存行をすべて削除し、SQL 結果を挿入。テーブル定義・インデックス・制約を保持。入力0件の場合も全行削除 |
| `merge` | `keys` の主キー・ユニークキーに一致する行を更新し、存在しなければ挿入。入力にない行を保持 |

`table`・`keys` は SQLite プラグインの options です。汎用のホスト API は `mode` をプラグインへ渡し、実際の反映方法をプラグインが実装します。ファイル出力の `mode` は従来どおり `error_if_exists` / `overwrite` です。

### 出力プラグイン API

スクリプトは `open_writer(options, columns, mode)` を返します。`columns` は SQL の列順に並ぶ配列で、各要素は `name`・`type`・`nullable` を持ちます。任意の `validate_output(options, columns, mode)` を定義すると、メタデータ検証時に options 等を検証できます。検証関数では接続・書き込みを行わないでください。

```lua
return {
    open_writer = function(options, columns, mode)
        -- ここで接続・トランザクションを開始する。
        return {
            write = function(row)     -- 必須。列名をキーにした1行を反映する。
            end,
            commit = function()       -- 必須。すべての行を確定する。
            end,
            rollback = function()     -- 必須。未確定の変更を取り消す。
            end,
            close = function()        -- 任意。接続等を解放する。
            end,
        }
    end,
}
```

各関数は `self` を渡さない通常の関数として呼び出します。戻り値の真偽ではなく、Lua の `error()` で失敗を通知します。カーソルを返す前に `open_writer` が失敗した場合の解放・ロールバックはプラグイン側で行ってください。

SQL の整数型はすべて `int64` / `uint64` として10進文字列で渡します。浮動小数点は有限の Lua 数値、真偽値は Boolean、文字列は UTF-8、バイナリは生の Lua 文字列、NULL は `nil` です。Decimal128 は桁を保持した文字列と `type: decimal`、追加の `precision` / `scale` を渡します。Date32・Time64・Timestamp は Arrow の日時表示による文字列と `type: date` / `time` / `timestamp` です。Time64 / Timestamp には `unit`（`Second` / `Millisecond` / `Microsecond` / `Nanosecond`）、Timestamp には任意の `timezone` も渡します。NULL 型の列は `type: null` で渡します。ネストした型等は SQL で対応する型に CAST してください。1行のデコードサイズは16 MiBまでです。

`check` / `explain` / `check --scan` は `open_writer`・`write`・`commit` を呼びません。`run` は全出力の SQL 計算と入力・出力 `check` を完了し、Lua 出力は Arrow IPC として一時領域に保存します。その後、すべてのファイル出力の公開権限を準備し、YAML の出力順でファイルの公開または外部テーブルへの書き込み・コミットを行います。一時領域には SQL 結果を保存できる空き容量が必要です。

1つの出力の `write` がすべて成功すると `commit`、続いて `close` を1回呼びます。失敗時は `rollback` と `close` を呼び、後続出力の公開を停止します。SIGINT / SIGTERM は書き込みループ内でも検査し、コミット前ならロールバックします。ネイティブ関数内での長時間のブロックや強制終了時の解放は、プラグイン・DB 側の動作に従います。

トランザクションは出力ごとです。複数の出力先に対する分散トランザクションは行わず、先に公開・コミット済みの出力は後続の失敗で取り消しません。エラーには `published`・`failed_external`・`not_started` を記録します。コミットエラーでは DB 側の確定結果が不明な場合があることを記録し、コミット後の close エラーは確定済みであることを記録します。

SQLite サンプルは既存 DB・既存テーブルを対象とし、`BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK` を使用します。`merge` は [SQLite UPSERT](https://www.sqlite.org/lang_upsert.html) で実装し、`keys` は SQL 結果に含まれる1列以上の主キー・ユニークキーです。複合キーに対応し、NULL キーはエラーです。キー以外の SQL 結果列だけを更新し、結果に含まれない既存列を保持します。同じキーが入力に複数回現れた場合は入力順に適用します。値はプレースホルダーへバインドし、64bit 整数は浮動小数点を経由しません。SQLite の整数範囲を超える UInt64 はエラーです。Decimal の厳密な文字列表現を保存する場合は保存先列を TEXT にしてください。`busy_timeout_ms` は既定5,000ミリ秒です。
