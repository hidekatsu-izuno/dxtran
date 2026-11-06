#!/usr/bin/env python3
"""dxt を使わず、標準ライブラリと明示した既知のバイト列で入力・期待値を作る。"""

import argparse
import gzip
import io
import json
from pathlib import Path
import struct


ROOT = Path(__file__).resolve().parent


def fixtures():
    files = {}

    def put(path, data, encoding="utf-8"):
        files[path] = data.encode(encoding) if isinstance(data, str) else data

    def binary(path, data):
        put(path, data)
        # バイナリや末尾空白をエディターでも確認できる補助ファイル。
        put(path + ".hex", data.hex(" ") + "\n")

    put("csv_to_csv/data/records.csv", 'id,memo,amount,active,business_date,payload\n'
        '0002,"comma,quote""",12.30,Y,20261026,00ff\n'
        '0001,"",-0.50,N,20240229,""\n'
        '0003,,0.00,Y,19700101,\n'
        '0004,"line\nbreak",1.25,N,20691231,aabb')
    put("csv_to_csv/expected/canonical.csv", 'id,memo,amount,active,business_date,payload\n'
        '0002,"comma,quote""",12.30,true,2026-10-26,00ff\n'
        '0001,"",-0.50,false,2024-02-29,""\n'
        '0003,,0.00,true,1970-01-01,\n'
        '0004,"line\nbreak",1.25,false,2069-12-31,aabb\n')
    put("csv_to_csv/expected/records.tsv", '"0002"\t"comma,quote"""\t12.30\tAP8=\r\n'
        '"0001"\t""\t-0.50\t""\r\n'
        '"0003"\t\t0.00\t\r\n'
        '"0004"\t"line\nbreak"\t1.25\tqrs=\r\n')

    put("csv_to_fixed/data/records.csv", "id,label,amount,active,business_date\n"
        "0002,東京,12.30,Y,20261026\n0001,A,-0.50,N,20240229\n0003,,,,\n")
    records = []
    for identifier, label, amount, active, date in [
        ("0002", "東京", "12.30", "Y", "20261026"),
        ("0001", "A", "-0.50", "N", "20240229"),
        ("0003", "", "0.00", " ", "        "),
    ]:
        record = (identifier.encode() + b"|" + label.encode().ljust(10)
                  + amount.encode().rjust(8) + active.encode() + date.encode() + b"|")
        assert len(record) == 33
        records.append(record + b"\n")
    binary("csv_to_fixed/expected/records.dat", b"".join(records))
    put("csv_to_fixed/expected/readback.csv", "id,label,amount,active,business_date\n"
        "0002,東京,12.30,true,2026-10-26\n0001,A,-0.50,false,2024-02-29\n0003,,0.00,,\n")

    sales = []
    for identifier, name, amount, status, note in [
        ("0002", "東京", "0012345", "A", "N/A"),
        ("0001", "大阪", "000005p", "A", "---"),
        ("0003", "名古屋", "0000000", "I", "OK "),
    ]:
        record = (identifier.encode() + name.encode("cp932").ljust(8)
                  + (amount + status + note).encode())
        assert len(record) == 23
        sales.append(record)
    # 最終レコード直後の CRLF 省略も検証する。
    binary("fixed_to_csv/data/sales.dat", b"\r\n".join(sales))
    put("fixed_to_csv/expected/sales.csv", "id,customer,amount,status,note\n"
        "0002,東京,123.45,A,\n0001,大阪,-0.50,A,\n0003,名古屋,0.00,I,OK\n")
    put("fixed_to_csv/expected/sales_cp932.tsv", '"0002"\t"東京"\t"123.45"\r\n'
        '"0001"\t"大阪"\t"-0.50"\r\n"0003"\t"名古屋"\t"0.00"\r\n', "cp932")

    names = ["東京", "大阪", ""]
    binary("fixed_to_fixed/data/names.dat",
           b"\r\n".join((name + "　" * ((8 - len(name.encode("cp932"))) // 2)).encode("cp932")
                          for name in names))
    binary("fixed_to_fixed/expected/names_utf16.dat",
           "".join(name + "　" * (4 - len(name)) + "\n" for name in names).encode("utf-16le"))
    binary("fixed_to_fixed/expected/names_utf8.dat",
           "".join(name + "　" * ((12 - len(name.encode())) // 3) for name in names).encode())
    put("fixed_to_fixed/expected/readback.csv", 'label\n東京\n大阪\n""\n')

    put("lines/data/records.txt", "01Alice\n02Bob\n03\n\n04Carol")
    put("lines/expected/records.txt", "01Alice\r\n02Bob\r\n03\r\n\r\n04Carol\r\n")
    put("lines/expected/padded.txt", "".join(
        record.ljust(10) + "\n" for record in ["01Alice", "02Bob", "03", "", "04Carol"]))
    put("lines/expected/records.csv", 'code,name\n01,Alice\n02,Bob\n03,""\n  ,""\n04,Carol\n')

    rdw_bodies = [b"01\x12\x3c" + "日本語".encode(), b"02\x04\x5dABC", b"03\x00\x0c"]

    def segment(flag, body):
        return struct.pack(">HBB", len(body) + 4, flag, 0) + body

    def block(*segments):
        return segment(0, b"".join(segments))

    plain = b"".join(segment(0, body) for body in rdw_bodies)
    binary("rdw_framing/data/records.rdw", plain)
    binary("rdw_framing/expected/records.rdw", plain)
    binary("rdw_framing/expected/records.vb", block(segment(0, rdw_bodies[0]))
           + block(segment(0, rdw_bodies[1]), segment(0, rdw_bodies[2])))
    # max_block_length=16: split the first record inside the second UTF-8 character.
    binary("rdw_framing/expected/records.vbs", block(segment(1, rdw_bodies[0][:8]))
           + block(segment(2, rdw_bodies[0][8:]))
           + block(segment(0, rdw_bodies[1])) + block(segment(0, rdw_bodies[2])))
    put("rdw_framing/expected/records.csv", 'code,amount,note\n01,123,日本語\n02,-45,ABC\n03,0,""\n')

    put("csv_dialects/data/names.csv", "0002;'東京;本店'\r\n0001;'O''Brien'\r\n0003;大阪", "cp932")
    canonical = "id,label\n0002,東京;本店\n0001,O'Brien\n0003,大阪\n"
    put("csv_dialects/data/names_utf16be.csv", b"\xfe\xff" + canonical.encode("utf-16be"))
    put("csv_dialects/expected/names_utf16.tsv", "id\tlabel\r\n0002\t東京;本店\r\n"
        "0001\tO'Brien\r\n0003\t大阪\r\n", "utf-16le")
    put("csv_dialects/expected/names_jis.csv", canonical, "iso-2022-jp")
    put("csv_dialects/expected/readback.csv", canonical)

    # 独立した既知の表現: 2の補数・IEEE は struct、HFP・ゾーン・パックは明示値。
    binary_rows = [
        b"AB  " + b" -12.30" + b"Y" + b"2026-10-26" + b"12:34:56.123456"
        + b"2026-10-26T12:34:56.123456Z" + bytes.fromhex("00ff8001")
        + struct.pack("<h", -2) + struct.pack(">h", -2)
        + struct.pack("<H", 258) + struct.pack(">H", 258)
        + struct.pack("<f", 1.5) + struct.pack(">d", -2.25)
        + bytes.fromhex("0000000000001041 c110000000000000b300000000000000")
        + bytes.fromhex("f1f2d3 2d313233 f1f2f360 f1f2f3 123d 123f 0123"),
        b"Z   " + b"   0.00" + b"N" + b"2024-02-29" + b"00:00:00.000000"
        + b"2024-02-29T00:00:00.000000Z" + bytes.fromhex("0a0d0000")
        + bytes(44) + bytes.fromhex("f0f0c0 2b303030 f0f0f04e f0f0f0 000c 000f 0000"),
    ]
    assert [len(row) for row in binary_rows] == [132, 132]
    records = b"".join(binary_rows)
    binary("binary_fields/data/records.bin", records)
    binary("binary_fields/expected/copy.bin", records)
    binary("binary_fields/expected/flags_lsb.bin", bytes.fromhex("feff0180 00000000"))
    header = ("label,amount,active,business_date,event_time,event_at,payload,first_flag,last_flag,"
              "signed_le,signed_be,unsigned_le,unsigned_be,ieee_le,ieee_be,ibm_le,ibm_be,"
              "trailing_sign,leading_sign,separate_sign,unsigned_zone,signed_packed,"
              "unsigned_packed,no_sign_packed\n")
    canonical = (header + "AB,-12.30,true,2026-10-26,12:34:56.123456,2026-10-26T12:34:56.123456Z,"
                 "00ff,true,true,-2,-2,258,258,1.5,-2.25,1,-1,-123,-123,-123,123,-12.3,12.3,12.3\n"
                 "Z,0.00,false,2024-02-29,00:00:00.000000,2024-02-29T00:00:00.000000Z,"
                 "0a0d,false,false,0,0,0,0,0,0,0,0,0,0,0,0,0.0,0.0,0.0\n")
    put("binary_fields/expected/records.csv", canonical)
    put("csv_to_binary/data/records.csv", canonical)
    binary("csv_to_binary/expected/records.bin", records)
    put("csv_to_binary/expected/readback.csv", canonical)

    put("null_defaults/data/seed.csv", "id\n1\n")
    defaults = (b" " * 4 + b"   0.00" + b" " * 53 + bytes(48)
                + bytes.fromhex("f0f0c0 2b303030 f0f0f04e f0f0f0 000c 000f 0000"))
    assert len(defaults) == 132
    binary("null_defaults/expected/defaults.bin", defaults)
    binary("null_defaults/expected/mixed_flags.bin", bytes.fromhex("8000"))

    put("datetime/data/events.csv", "business_date,event_time,event_at,local_at\n"
        "700101,235959.123456789,2026-10-26T00:30:00.123+09:00,20261026003000\n"
        "691231,000000.000000001,2024-02-29T12:00:00.000Z,20240229120000\n")
    compact = ("19700101" + "235959.123456789" + "20261026003000.123+0900" + "20261026003000\r\n"
               + "20691231" + "000000.000000001" + "20240229210000.000+0900" + "20240229120000\r\n")
    assert all(len(row) == 61 for row in compact.splitlines())
    binary("datetime/expected/events.dat", compact.encode())
    put("datetime/expected/events_utc.csv", "business_date,event_time,event_at,local_at\n"
        "1970-01-01,23:59:59.123456789,2026-10-25T15:30:00.123Z,2026-10-26T00:30:00\n"
        "2069-12-31,00:00:00.000000001,2024-02-29T12:00:00.000Z,2024-02-29T12:00:00\n")
    put("datetime/expected/readback.csv", "business_date,event_time,event_at,local_at\n"
        "1970-01-01,23:59:59.123456789,2026-10-26T00:30:00.123+09:00,2026-10-26T00:30:00\n"
        "2069-12-31,00:00:00.000000001,2024-02-29T21:00:00.000+09:00,2024-02-29T12:00:00\n")

    binary("join_aggregate/data/sales.dat",
           b"00020012345A00010000500A00020000100A00030000999I00090000100A")
    put("join_aggregate/data/customers.csv", "customer_id,customer_name,region\n"
        "0001,Alice,east\n0002,Bob,west\n0003,Carol,east\n")
    put("join_aggregate/expected/detail.csv", '"customer_id",customer_name,region,amount\n'
        '"0002",Bob,west,123.45\n"0001",Alice,east,5.00\n"0002",Bob,west,1.00\n')
    binary("join_aggregate/expected/totals.dat", b"east 001000000500\r\nwest 002000012445\r\n")

    put("empty/data/header_only.csv", "id\n")
    put("empty/data/empty.dat", b"")
    put("empty/expected/header_only.csv", "id\n")
    put("empty/expected/empty.dat", b"")
    put("empty/expected/no_header.csv", b"")
    put("errors/data/malformed.csv", 'value\n"unclosed\n')
    put("errors/data/short.dat", b"ABCDXYZ")
    put("errors/data/wide.csv", "value\n東京\n")
    put("errors/data/decimal.csv", "amount\n12.34\n")
    put("errors/data/unicode.csv", "value\n😀\n")
    parquet_csv = ('id,memo,amount,payload\n'
                   '0002,"日本,語",12.30,00ff\n'
                   '0001,"",-0.50,""\n'
                   '0003,,0.00,\n'
                   '0004,"line\nbreak",1.25,aabb\n')
    put("parquet/data/records.csv", parquet_csv)
    put("parquet/expected/readback.csv", parquet_csv)
    fixed_records = b"0002" + "東京".encode("cp932").ljust(8) + b"00001230\r\n" + b"0001A       00000050\r\n"
    for name, data in [("records.csv.gz", parquet_csv.encode()), ("records.dat.gz", fixed_records)]:
        stream = io.BytesIO()
        # Stored deflate blocks avoid version-dependent compression choices in fixtures.
        with gzip.GzipFile(fileobj=stream, filename="", mode="wb", compresslevel=0, mtime=0) as writer:
            writer.write(data)
        put("compression/data/" + name, stream.getvalue())
    put("compression/expected/csv.csv", parquet_csv)
    put("compression/expected/fixed.csv", "id,label,amount\n0002,東京,12.30\n0001,A,0.50\n")
    conditional_records = [
        b"H20261028DXTRAN ",
        b"D0000001   12.30",
        b"D0000002   -0.50",
        b"T      2   11.80",
    ]
    assert all(len(record) == 16 for record in conditional_records)
    fixed_records = b"".join(record + b"\n" for record in conditional_records)
    binary("conditional_records/data/records.dat", fixed_records)
    binary("conditional_records/expected/records.dat", fixed_records)
    binary("conditional_records/expected/records.rdw", b"".join(
        struct.pack(">H", len(record) + 4) + b"\x00\x00" + record
        for record in conditional_records))
    put("conditional_records/expected/records.csv",
        "record_type,business_date,sender,customer_id,amount,detail_count,total_amount\n"
        "H,2026-10-28,DXTRAN,,,,\n"
        "D,,,0000001,12.30,,\n"
        "D,,,0000002,-0.50,,\n"
        "T,,,,,2,11.80\n")
    put("checks/data/records.csv", "id,amount\nA001,12.30\nA002,-0.50\nA003,\n")
    put("checks/expected/records.csv", "id,amount\nA001,24.60\nA002,-1.00\nA003,\n")
    put("exit_codes/data/records.csv", "id,amount\nA001,12.30\nA002,-0.50\n")
    put("exit_codes/expected/records.csv", "id,amount\nA001,12.30\nA002,-0.50\n")
    # Known EBCDIC bytes, including payloads that need not be valid text.
    bstrings = [bytes.fromhex(value) for value in [
        "f0404040ff00", "c140404000ff", "814040400e45"]]
    binary("bstring/data/records.dat", b"".join(bstrings))
    put("bstring/data/lookup.csv", "code,label\nA,大文字\na,小文字\n0,数字\n")
    binary("bstring/expected/records.dat", b"".join(bstrings))
    binary("bstring/expected/records.rdw", b"".join(
        struct.pack(">H", len(record) + 4) + b"\x00\x00" + record for record in bstrings))
    put("bstring/expected/sorted.csv", "code_text,payload\na,0e45\nA,00ff\n0,ff00\n")
    put("bstring/expected/joined.csv", "code_text,label,payload\na,小文字,0e45\nA,大文字,00ff\n0,数字,ff00\n")
    put("bstring/expected/readback.csv", "code,payload\nf0,ff00\nc1,00ff\n81,0e45\n")
    fullwidth = [("cp932", 8, 4), ("jef", 8, 4), ("utf-16le", 8, 4),
                 ("utf-16be", 8, 4), ("cp930", 10, 3), ("cp939", 10, 3),
                 ("iso-2022-jp", 14, 1)]
    put("bstring/data/labels.csv", "encoding,label\n" + "".join(
        f"{encoding},　日本　\n" for encoding, _, _ in fullwidth))
    put("bstring/expected/fullwidth.csv", "encoding,original_bytes,trimmed,padded,padded_bytes\n" + "".join(
        f"{encoding},{original},日本,日本{'　' * padding},12\n"
        for encoding, original, padding in fullwidth))
    put("validation/data/sales.csv", "顧客コード,売上金額,地域\nC01,10,east\nC02,20,east\nC03,30,west\n")
    put("validation/expected/records.csv", "customer_id,amount,business_date\nC01,10,2026-11-06\nC02,20,2026-11-06\n")
    return files


def manifest():
    def job(file, *outputs):
        return {"file": file, "outputs": {"out/" + name: "expected/" + name for name in outputs}}

    cases = [
        ("validation", [{"file": "job.yml", "outputs": {
            "out/records_2026-11-06.csv": "expected/records.csv"}}]),
        ("exit_codes", [job("job.yml", "records.csv"),
            {"file": "input_error.yml", "exit_code": 12, "scan_exit_code": 12,
             "diagnostic": "入力金額が負のため終了コード12で終了します。", "absent": ["out/input_error.csv"]},
            {"file": "output_error.yml", "exit_code": 20,
             "diagnostic": "出力金額が負のため終了コード20で終了します。", "absent": ["out/output_error.csv"]}]),
        ("rdw_framing", [
            {"file": "job.yml", "outputs": {
                "out/records.csv": "expected/records.csv", "out/records.rdw": "expected/records.rdw",
                "out/records.vb": "expected/records.vb", "out/records.vbs": "expected/records.vbs",
                "out/records.sdw": "expected/records.rdw"}},
            {"file": "readback.yml", "outputs": {
                "out/plain.csv": "expected/records.csv", "out/blocked.csv": "expected/records.csv",
                "out/spanned.csv": "expected/records.csv", "out/segments.csv": "expected/records.csv"}}]),
        ("lines", [job("job.yml", "records.csv", "records.txt", "padded.txt"),
            {"file": "readback.yml", "outputs": {"out/readback.csv": "expected/records.csv"}}]),
        ("csv_to_csv", [job("job.yml", "canonical.csv", "records.tsv")]),
        ("csv_to_fixed", [job("job.yml", "records.dat"), job("readback.yml", "readback.csv")]),
        ("fixed_to_csv", [job("job.yml", "sales.csv", "sales_cp932.tsv")]),
        ("fixed_to_fixed", [job("job.yml", "names_utf16.dat", "names_utf8.dat"),
                            job("readback.yml", "readback.csv")]),
        ("csv_dialects", [job("job.yml", "names_utf16.tsv", "names_jis.csv"),
                          {"file": "readback.yml", "outputs": {
                              "out/utf16_readback.csv": "expected/readback.csv",
                              "out/jis_readback.csv": "expected/readback.csv",
                              "out/bom_readback.csv": "expected/readback.csv"}}]),
        ("binary_fields", [job("job.yml", "records.csv", "copy.bin", "flags_lsb.bin")]),
        ("csv_to_binary", [job("job.yml", "records.bin"), job("readback.yml", "readback.csv")]),
        ("null_defaults", [job("job.yml", "defaults.bin", "mixed_flags.bin")]),
        ("datetime", [job("job.yml", "events.dat", "events_utc.csv"), job("readback.yml", "readback.csv")]),
        ("join_aggregate", [job("job.yml", "detail.csv", "totals.dat")]),
        ("empty", [job("job.yml", "header_only.csv", "empty.dat", "no_header.csv")]),
        ("parquet", [{"file": "job.yml", "produced": ["out/records.parquet"]},
                     job("readback.yml", "readback.csv")]),
        ("compression", [{"file": "job.yml", "produced": [
            "out/records.csv.gz", "out/records.csv.zst", "out/records.dat.gz", "out/records.dat.zst"]},
            {"file": "readback.yml", "outputs": {
                "out/csv_gzip.csv": "expected/csv.csv", "out/csv_zstd.csv": "expected/csv.csv",
                "out/fixed_gzip.csv": "expected/fixed.csv", "out/fixed_zstd.csv": "expected/fixed.csv"}}]),
        ("conditional_records", [job("job.yml", "records.csv", "records.dat", "records.rdw"),
            {"file": "readback.yml", "outputs": {
                "out/fixed_readback.csv": "expected/records.csv",
                "out/rdw_readback.csv": "expected/records.csv"}}]),
        ("checks", [job("job.yml", "records.csv"),
            {"file": "input_abend.yml", "exit_code": 3, "scan_exit_code": 3,
             "diagnostic": "入力金額が負のため異常終了します。", "absent": ["out/input_abend.csv"]},
            {"file": "output_abend.yml", "exit_code": 5,
             "diagnostic": "変換後の金額が負のため異常終了します。", "absent": ["out/output_abend.csv"]}]),
        ("bstring", [job("job.yml", "records.dat", "records.rdw", "sorted.csv", "joined.csv"),
            job("fullwidth.yml", "fullwidth.csv"),
            {"file": "readback.yml", "outputs": {
                "out/fixed_readback.csv": "expected/readback.csv",
                "out/rdw_readback.csv": "expected/readback.csv"}}]),
    ]
    errors = []
    for file, code, scan, diagnostic, absent in [
        ("malformed_csv.yml", 3, 3, "unclosed CSV quote", ["malformed.csv"]),
        ("short_fixed.yml", 3, 3, "truncated record", ["short.csv"]),
        ("width_overflow.yml", 5, 0, "exceeds field width", ["first.csv", "too_narrow.dat"]),
        ("precision_loss.yml", 5, 0, "scale would lose nonzero digits", ["precision.dat"]),
        ("unrepresentable.yml", 5, 0, "character cannot be represented", ["cp932.csv"]),
    ]:
        errors.append({"file": file, "exit_code": code, "scan_exit_code": scan,
                       "diagnostic": diagnostic, "absent": ["out/" + name for name in absent]})
    cases.append(("errors", errors))
    return [{"directory": directory, "jobs": jobs} for directory, jobs in cases]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="ファイルを変更せず入力・期待値の一致を確認")
    args = parser.parse_args()
    files = fixtures()
    files["manifest.json"] = (json.dumps(manifest(), ensure_ascii=False, indent=2) + "\n").encode()
    mismatches = []
    for name, expected in files.items():
        path = ROOT / name
        if args.check:
            if not path.is_file() or path.read_bytes() != expected:
                mismatches.append(name)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(expected)
    if mismatches:
        parser.exit(1, "fixture mismatch:\n" + "\n".join(mismatches) + "\n")
    print(f"{'Verified' if args.check else 'Generated'} {len(files)} fixture files")


if __name__ == "__main__":
    main()
