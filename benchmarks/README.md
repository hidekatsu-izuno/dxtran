# 入出力の性能比較

## CSV

[CSV 高速化の測定結果](CSV.md)と[全試行のデータ](results/csv-comparison-256mib.json)を保存しています。

```sh
# 変更前の release バイナリを別の場所に保存してから実行
python3 -B benchmarks/csv_io.py --size-mib 256 --threads 1 4 --repeats 5 \
  --compare-dxt /path/to/before/dxt \
  --result benchmarks/results/csv-comparison-256mib.json
```

UTF-8 の文字列1項目、文字列8項目＋Decimal128 8項目、引用符・埋め込み改行・日本語・NULL・空文字を含む16項目を比較します。ヘッダーなしの `SELECT * FROM data` で、出力が入力と同じバイト列になるデータです。可変長のケースは256 MiB以内で最後の完全なレコードまで生成します。ウォームキャッシュ、出力の同期・公開を含むCLI全体を計測し、各試行で出力サイズとSHA-256を検証します。各ケースの実行順をシャッフルし、全試行・CPU時間・最大RSS・実行計画・バイナリハッシュをJSONに保存します。

## 固定長 READ/WRITE

[固定長変換の並列化](PARALLEL_FIXED.md)、[Arrow 直接変換の比較](DIRECT_ARROW.md)、[最初の高速化前後の比較](OPTIMIZATION.md)、[初回の測定結果](RESULTS.md) と、生データを保存しています。

同じ256バイト固定長ファイルを、次の方式で読み書きします。

| 方式 | 処理内容 |
| --- | --- |
| block_copy_1mib | Rust の明示的な read/write を1MiB単位で実行。カーネルのファイルコピー専用機能は使わない |
| record_copy_256b | Rust のバッファ付き READ/WRITE を256バイトのレコード単位で実行。項目の解析なし |
| codecs_one_field / codecs_sixteen_fields | dxtran の Reader と項目変換を使い、Arrow・SQL を通さず再出力 |
| dxt_one_field_threads_1 / 4 | 256バイトの文字列1項目として `SELECT * FROM data` を実行 |
| dxt_sixteen_fields_threads_1 / 4 | 16バイトの文字列8項目と、16バイトの ASCII uzoned（precision 16、scale 2）8項目で同じ SQL を実行 |

入力はUTF-8、改行なし、NULLなしです。1項目の文字列は `trim: none` とし、全方式で入力と同じバイト列を出力します。データは4,096種類のレコードを繰り返したもので、ランダムデータではありません。

すべての方式で出力ファイルの `fsync`、一時ファイルからの rename、公開前後の親ディレクトリの `fsync` を行います。`dxt` は設定読み込み、計画、実行専用ディレクトリの作成・削除も含む通常の CLI をそのまま測ります。コピーと codec の比較用プログラムには SQL 計画や実行専用ディレクトリの管理はありません。

経過時間はプロセスの起動から終了までです。転送速度は「入力 MiB ÷ 経過秒」であり、読み書き合計のディスク帯域ではありません。最大 RSS・ユーザー／システム CPU 時間・ファイルシステム I/O 量は `/usr/bin/time` で記録します。出力サイズと SHA-256 の一致確認は計測時間の外で毎回実施します。

```sh
# release ビルド → ウォームアップ各1回 → 順番を入れ替えて各5回計測
python3 benchmarks/fixed_io.py --size-mib 256 --result benchmarks/results/current-warm-256mib.json

# 入力ページキャッシュの破棄を OS に助言する条件も測る
python3 benchmarks/fixed_io.py --skip-build --size-mib 256 --repeats 3 \
  --cache cold-advisory --result benchmarks/results/current-cold-advisory-256mib.json
```

変更前後の比較では、変更前の release バイナリを別の場所に保存し、変更後のビルドで次を実行できます。
`--compare-dxt` / `--compare-codec` を渡すと、`before_` 付きの方式も同じ入力・条件・実行順のシャッフルに含めます。
比較用バイナリの SHA-256 も保存します。

`--threads 1 2 4 8` のように比較する並列度を指定できます。省略時は従来どおり 1 と 4 です。

```sh
python3 benchmarks/fixed_io.py --size-mib 256 --repeats 5 \
  --compare-dxt /path/to/before/dxt \
  --compare-codec /path/to/before/fixed_codec_copy \
  --result benchmarks/results/optimized-comparison-256mib.json
```

通常の条件は、各実行直前に入力を読み込んだウォームキャッシュです。`cold-advisory` は `POSIX_FADV_DONTNEED` による助言で、OS による破棄や物理ドライブ・WSL ホスト側のキャッシュ破棄を保証しません。システム全体のキャッシュは破棄しません。

入力・出力・一時領域は同じファイルシステム上の実行専用ディレクトリに置き、終了時に削除します。保存する JSON には全試行、中央値、最小／最大時間、RSS、CPU 時間、spill 件数、ハードウェア情報、実行バイナリのハッシュ、実行計画を含めます。測定中はビルドや別の負荷試験を同時に実行しないでください。
