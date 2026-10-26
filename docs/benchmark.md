# ベンチマーク

測定日: 2026-10-26。Parquet入力・CSV変換の並列化まで実装したrelease版の結果です。

**固定長16項目を4スレッドで変換する場合、単純READ/WRITEの約1.39倍の時間（約39%遅い）でした。** 256 MiBで、単純READ/WRITEが **0.265秒**、dxtが **0.368秒**、差は **0.103秒**です。下記の条件での中央値であり、任意のデータ・ストレージに対する保証値ではありません。

## 固定長の単純READ/WRITEとの差

同じ入力ファイルに対し、単純READ/WRITEと `SELECT * FROM data` を比較しました。dxtは両レイアウトとも `--threads 4 --memory-limit 512MiB`。各方式ウォームアップ1回＋本測定5回、順番をシャッフルし、試行間に計測対象外の1秒休止を置いた確認測定です。

「時間倍率」は `dxtの経過時間中央値 ÷ 単純READ/WRITEの経過時間中央値`。1より大きいほど遅いことを表します。CPU時間はuser＋systemの合計で、並列処理では経過時間より長くなります。

| 方式 | 経過時間 中央値 秒 | 時間倍率 | 最小〜最大 秒 | CPU中央値 秒 | 最大RSS中央値 MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 単純READ/WRITE（256バイト単位） | 0.265 | 1.00倍 | 0.248〜0.322 | 0.20 | 2.1 |
| dxt：UTF-8文字列1項目 | 0.435 | 1.64倍 | 0.297〜0.970 | 0.32 | 57.7 |
| dxt：文字列8＋数値8項目 | 0.368 | 1.39倍 | 0.318〜0.454 | 1.17 | 85.9 |

16項目の変換速度は約696 MiB/sです。並列化で実時間の差を抑えていますが、CPU時間は単純READ/WRITEの約5.8倍、最大RSSは約86 MiBです。文字列1項目は変換の並列化を使わず、実時間にも大きな揺れがあるため、この表から「16項目の方が本質的に軽い」とは判断できません。

### 比較する処理

- **単純READ/WRITE:** Rustの `BufReader` / `BufWriter` で256バイトのレコードをそのまま読み書き。文字コード変換・項目検証・Arrow配列・SQL処理を行わない。カーネルのファイルコピー専用機能は使わない。
- **dxt 16項目:** 文字列8項目＋ASCII uzoned数値8項目、各16バイト。数値はprecision 16、scale 2。全項目をデコード・検証し、ArrowとSQLを経由して元の固定長形式へ戻す。行順を維持する。
- **dxt 1項目:** 全256バイトをUTF-8文字列1項目（`trim: none`）として変換する。
- どの方式も入力・出力は同一のバイト列になる。書き込み完了後のファイル同期、rename、親ディレクトリ同期まで含める。dxtでは設定読み込み・計画・一時領域の作成と削除も含む。

[確認測定の全18試行](../benchmarks/results/fixed-current-confirmation-256mib.json) / [単純READ/WRITEの実装](../benchmarks/fixed_copy.rs) / [測定スクリプト](../benchmarks/fixed_io.py)。

## CSV・Parquetとの参考比較

同じreleaseバイナリ、4スレッドで、CSV換算256 MiB・1,048,576行の文字列8項目＋Decimal128 8項目を全件CSVへ出力した結果です。表の中央値は本測定と追加確認の計10回から計算しています。

**この表の倍率は別測定の固定長READ/WRITE（0.265秒）を分母にした参考値です。** データ、ファイル表現、測定系列が異なり、形式固有の追加コストを厳密に分離した比較ではありません。Parquetの実入力サイズはSnappyが約195.2 MiB、Zstdが約129.4 MiBで、出力CSVはいずれも256 MiBです。

| 入力 → 出力 | 経過時間 中央値 秒 | 固定長READ/WRITE基準の参考倍率 | 最小〜最大 秒 |
| --- | ---: | ---: | ---: |
| CSV → CSV | 0.687 | 2.59倍 | 0.646〜1.225 |
| Parquet（Snappy、8,192行グループ）→ CSV | 0.509 | 1.92倍 | 0.450〜1.398 |
| Parquet（Zstd、65,536行グループ）→ CSV | 0.521 | 1.96倍 | 0.468〜0.740 |

今回の並列化前と同じ条件で比較した速度向上は、CSV全件変換が **1.39倍**、入力中心のSUM集計が **CSVで1.69倍、Parquetで1.39〜1.75倍**でした。Parquet入力から全件CSV出力する場合は、入力と出力整形の両方の改善を含めて **1.64〜2.11倍**です。集計は出力行数が異なるため、単純READ/WRITEとの時間倍率には含めていません。

[並列化の詳細・CPUとRSS・1/4/8スレッド比較](../benchmarks/PARALLEL_FORMATS.md) / [主測定216試行](../benchmarks/results/parallel-formats-comparison-256mib.json) / [追加確認72試行](../benchmarks/results/parallel-formats-confirmation-256mib.json)。引用符・改行・NULLを含むCSVや文字列1項目は[別の72試行](../benchmarks/results/parallel-csv-comparison-256mib.json)で確認しています。

## 実時間のばらつきと最初の測定

最初に、固定長の1・4・8スレッドと2種類の単純コピーをまとめて各9回測定しました。この系列では、単純READ/WRITEだけでも約0.25〜2.10秒に揺れました。CPU時間に比べ経過時間が大きく、I/O等の待ち時間の影響を強く受けた結果です。

| 方式 | 経過時間 中央値 秒 | 最小〜最大 秒 | CPU中央値 秒 | 同系列のREAD/WRITE基準 |
| --- | ---: | ---: | ---: | ---: |
| 単純READ/WRITE | 1.336 | 0.250〜2.102 | 0.21 | 1.00倍 |
| dxt 16項目 / 1スレッド | 2.461 | 0.981〜3.074 | 0.87 | 1.84倍 |
| dxt 16項目 / 4スレッド | 1.026 | 0.339〜2.448 | 1.11 | 0.77倍 |
| dxt 16項目 / 8スレッド | 1.250 | 0.292〜2.133 | 1.47 | 0.94倍 |

この系列ではdxt 16項目・4スレッドの時間倍率が0.77倍ですが、これを「dxtが単純READ/WRITEより速い」という結論には使っていません。比較対象を3方式に絞り、直前の測定から30秒以上空け、試行間に1秒休止を置いて再確認したものが冒頭の表です。両系列は条件が異なるため混ぜず、どちらも外れ値を除かず保存しています。

[最初の固定長測定・全80試行](../benchmarks/results/fixed-current-256mib.json)。CPU時間とRSSも含め、実運用の文字コード・項目数・SQL・ストレージで評価してください。

## 共通条件・検証

- AMD Ryzen AI 9 465、20論理CPUが利用可能、WSL2（Linux 6.18.54.1）、ext2/ext3と報告される同一ファイルシステム上で入力・一時領域・出力を作成。
- Rust 1.99.1、DataFusion 55.2.0、Arrow / Parquet 59.3.1、releaseビルド。
- 固定長は256 MiB＝268,435,456バイト、256バイト×1,048,576行、UTF-8、区切りなし。4,096種類の行を繰り返す。CSV/Parquet主測定は行ごとに異なる値を使う。
- 入力は各試行直前に読み込むウォームキャッシュ。物理ディスクからのコールド読み込み速度ではない。ビルドや別の負荷試験を同時に実行しない。
- 計測はCLIの起動から終了まで。入力生成・出力検証・試行間の休止は除く。転送速度は入力相当MiB÷秒で、読み書き合計のディスク帯域ではない。
- 固定長は全98試行で入力と出力のサイズ・SHA-256が一致。CSV/Parquetの最終測定は全360試行で独立生成した期待値と一致。spillは全件0。全92テストとClippyが成功。
- `--memory-limit` はDataFusion管理下の上限。並列変換・Parquetの辞書やページ等は管理外であり、RSSの厳密な上限ではない。
- gzip/zstdで圧縮したCSV・固定長の速度、およびParquet出力の速度は今回の比較対象外。

測定したdxtのSHA-256:

```text
196c2e597b0b51e478be6915bd60c206e7fe6aff3fae3d27f0fcf0be23ee14c4
```

## 再現

```sh
cargo build --release --locked --offline --bin dxt -j3

# 冒頭の比較。開始前は他の負荷試験を止め、30秒以上空ける。
python3 -B benchmarks/fixed_io.py --skip-build --skip-codec \
  --size-mib 256 --threads 4 --repeats 5 --pause-ms 1000 \
  --cases record_copy_256b dxt_one_field_threads_4 dxt_sixteen_fields_threads_4 \
  --result benchmarks/results/fixed-current-confirmation-256mib.json

# 最初の全条件測定
python3 -B benchmarks/fixed_io.py --skip-build --skip-codec \
  --size-mib 256 --threads 1 4 8 --repeats 9 \
  --result benchmarks/results/fixed-current-256mib.json
```

CSV・Parquetの再現手順は [benchmarks/PARALLEL_FORMATS.md](../benchmarks/PARALLEL_FORMATS.md#再現と保存先) に記載しています。
