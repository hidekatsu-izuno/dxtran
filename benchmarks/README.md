# 入出力の性能比較

改善後の固定長ファイルの単純READ/WRITEに対する時間差・倍率は [docs/benchmark.md](../docs/benchmark.md) を参照してください。

## Parquet入力・CSV変換の並列化

[並列化の測定結果](PARALLEL_FORMATS.md)と[全試行のデータ](results/parallel-formats-comparison-256mib.json)を保存しています。

```sh
python3 -B benchmarks/parallel_formats.py --size-mib 256 --threads 1 4 8 --repeats 5 \
  --compare-dxt /path/to/before/dxt \
  --result benchmarks/results/parallel-formats-comparison-256mib.json
```

Rust製の `parquet_fixture` exampleで、行ごとに値が異なる文字列8項目＋Decimal128 8項目のCSVとParquetを作ります。CSV換算で1行256バイトです。ParquetはSnappy・8,192行グループとZstd・65,536行グループを使用します。各入力の全件CSV出力と、数値8列のSUM集計を比較し、入力中心の処理とCSV出力を含む処理を分けて評価します。SUMでも使用しない文字列列を含め全列をデコードします。

出力は、dxtを使わず生成したCSVのサイズ・SHA-256、またはPythonで独立計算した集計値と毎回照合します。測定はウォームキャッシュでCLI起動から同期・公開・終了までを含め、順番をシャッフルします。CPU時間、最大RSS、spill、実行計画、入力・バイナリのハッシュも記録します。入力データ作成・出力検証は計測対象外です。全結果の転送速度はCSV換算で、Parquetファイル自体のディスク帯域とは異なります。

## check・少数列選択・大量process

共通化・チェック処理・process計画の改善と変更前後の比較は、[共通化と実行コストの改善](REVIEW_IMPROVEMENTS.md)を参照してください。比較スクリプトは `--compare-dxt /path/to/before/dxt` に対応し、連鎖するprocess、SQL関数、バイナリ・条件付きレイアウトも測定します。

```sh
python3 -B benchmarks/review_workloads.py --rows 65536 --threads 1 4 --repeats 5 \
  --process-counts 100 1000 --result benchmarks/results/review-workloads.json
```

Parquet入力でcheckなし／入力check 1件・3件／出力check 1件・3件を比較します。ルールは正常なデータに一致しない条件とし、全ケースの出力を独立生成したCSVのサイズ・SHA-256と照合します。100列のParquetでは全列出力と2列出力を測ります。どちらも現行仕様どおり未使用列を含む全列を検証・デコードするため、選択列だけの読み込みの効果を示す測定ではありません。

100件・1,000件の独立したprocessは `check` による設定読込・SQL計画全体を測り、依存順序解決だけの時間とは区別します。行順・ルール順のエラーやメッセージの確定方法は変更していません。

Linuxの `/usr/bin/time` を使い、ウォームキャッシュ、同期・公開を含むCLI全体の経過時間、CPU時間、最大RSS、行/秒、実行計画、入力・バイナリのハッシュを記録します。データ生成・出力検証は計測外です。全試行を順序シャッフルして保存します。小規模の動作確認には `--profile debug --rows 1024 --repeats 1 --warmups 0` を使えますが、debug版の数値は性能評価に使いません。

2026-11-06の初回測定は、`jtc-utils 0.2.1`・`encoding_rs 0.8.44` のrelease版、65,536行、1/4スレッド、各ウォームアップ1回＋本測定3回です。[全72試行と環境・実行計画](results/review-workloads-65536.json)を保存しました。全試行で期待出力と一時領域の清掃を確認しています。以下は4スレッドの中央値で、16列はCSV換算16 MiB、100列は100 MiBの入力です。

| ケース | 経過秒 | CPU秒 | 行/秒 |
| --- | ---: | ---: | ---: |
| 16列・checkなし | 0.0741 | 0.11 | 884,111 |
| 16列・入力check 1件 | 0.1858 | 0.18 | 352,806 |
| 16列・入力check 3件 | 0.2236 | 0.24 | 293,035 |
| 16列・出力check 1件 | 0.2884 | 0.27 | 227,224 |
| 16列・出力check 3件 | 0.3356 | 0.34 | 195,289 |
| 100列から100列を出力 | 0.6908 | 0.64 | 94,869 |
| 100列から2列を出力 | 0.1123 | 0.25 | 583,797 |

独立processの計画全体は100件が0.0167秒、1,000件が0.0721秒でした。check付き処理の追加コストには行ごとの式評価や書き出し、入力checkでの並列デコード抑制も含みます。100列から2列への変換では出力・ソートする列数が減りますが、未使用列もデコードしています。これらは現在の実装の測定値であり、変更前との速度比較や任意のジョブの性能保証には使いません。

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
