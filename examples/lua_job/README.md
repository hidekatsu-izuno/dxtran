# ジョブ全体で Lua の変数を共有する

Lua は既定で無効です。内容を確認したサンプルに限り、`--allow-lua` を付けて実行します。これはサンドボックスではありません。[セキュリティ境界](../../docs/LUA_SECURITY.md)を参照してください。

[job.yml](job.yml) のジョブ直下に `lua.variables` を指定し、各プラグインから同じ `dxt.job` テーブルを参照します。DB 専用の設定はありません。数値・文字列・真偽値・テーブルを共有でき、初期化処理で作った LuaJIT FFI の cdata 等も保持できます。

```yaml
lua:
  variables:
    label: example
    read_rows: 0
    saved_rows: 0
```

[table.lua](table.lua) は入力を読むたびに `dxt.job.read_rows` を更新し、出力を確定すると `dxt.job.saved_rows` を更新します。

初期化・終了処理を使う場合だけ `lua.path` を指定します。[lifecycle.lua](lifecycle.lua) は開始時に集計ファイルのパスを設定し、終了時に共有変数の値を書き出します。終了関数は `close(job, success)` で、入力カーソル・出力 writer を閉じた後に呼び出します。

```sh
cargo build --locked --bin dxt
mkdir -p examples/lua_job/out
target/debug/dxt check --allow-lua examples/lua_job/job.yml
target/debug/dxt explain --allow-lua examples/lua_job/job.yml
target/debug/dxt run --allow-lua examples/lua_job/job.yml --threads 4
cmp examples/lua_job/expected/records.csv examples/lua_job/out/records.csv
cmp examples/lua_job/expected/summary.txt examples/lua_job/out/summary.txt
```

2つの出力がそれぞれ入力を走査するので、読み取り回数は4行、確定したカウント用出力は2行です。`run` ごとに共有変数を初期値から作り直します。

`check` / `explain` はフックを呼びません。`check --scan` は `init` / `close` も呼ぶため、この例では `summary.txt` が生成されます。出力 writer は開かず、その場合の集計は `read_rows=2`・`saved_rows=0` です。

[共有変数とライフサイクル API](../../docs/LUA_PLUGINS.md#ジョブ共有の変数)を参照してください。
