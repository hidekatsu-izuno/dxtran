mod batch;
pub(crate) mod lua;
mod reader;
mod table;
pub use reader::InputReader;
pub use table::SourceTable;
mod csv;
mod fixed;
mod parallel_csv;
mod parquet;
pub use csv::CsvReader;

#[derive(Debug, PartialEq)]
pub struct Cell {
    pub value: String,
    pub was_quoted: bool,
}

#[cfg(test)]
mod tests {
    use super::batch::order_key;
    use super::*;
    use crate::framing;
    use crate::{config::Dataset, layout::RecordLayout};
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, SchemaRef},
        common::ScalarValue,
    };
    use serde_json::json;
    use std::sync::Arc;

    fn test_reader(
        mut config: serde_json::Value,
        data: &[u8],
    ) -> (tempfile::TempDir, InputReader, SchemaRef) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input");
        std::fs::write(&path, data).unwrap();
        config["path"] = json!(path);
        config["id"] = json!("data");
        let dataset: Dataset = serde_json::from_value(config).unwrap();
        let layout = RecordLayout::input(&dataset).unwrap();
        let mut fields = layout.schema.fields().to_vec();
        fields.push(Arc::new(Field::new("order_key", DataType::Utf8, false)));
        let schema = Arc::new(Schema::new(fields));
        let reader = InputReader::open(&dataset, &layout).unwrap();
        (dir, reader, schema)
    }

    #[test]
    fn direct_batches_preserve_all_field_types_and_nulls() {
        let cases = [
            ("string", "41422020"),
            ("decimal", "202020313233"),
            ("boolean", "7472756520"),
            ("date", "323032362d31302d3235"),
            ("time", "31323a33343a35362e313233343536"),
            (
                "timestamp",
                "323032362d31302d32355431323a33343a35362e3132333435365a",
            ),
            ("binary", "00ff"),
            ("bits", "8001"),
            ("int-le", "feff"),
            ("int-be", "fffe"),
            ("uint-le", "0201"),
            ("uint-be", "0102"),
            ("float-le", "0000803f"),
            ("float-be", "3ff0000000000000"),
            ("hfloat-le", "0000000000001041"),
            ("hfloat-be", "41100000000000003300000000000000"),
            ("zoned", "f1f2c3"),
            ("lzoned", "4ef1f2f3"),
            ("tzoned", "f1f2f34e"),
            ("uzoned", "f1f2f3"),
            ("packed", "123c"),
            ("upacked", "123f"),
            ("npacked", "0123"),
        ];
        let mut fields = Vec::new();
        let mut normal = Vec::new();
        for (i, (kind, hex)) in cases.into_iter().enumerate() {
            let bytes = hex::decode(hex).unwrap();
            let mut field = json!({"name":format!("f{i}"),"type":kind,"length":bytes.len(),"nullable":true,"null_bytes":"ff".repeat(bytes.len())});
            if kind == "bits" {
                field["bits"] = json!([{"name":"b0","bit":0},{"name":"b15","bit":15}]);
            }
            fields.push(field);
            normal.extend(bytes);
        }
        let null = vec![255; normal.len()];
        let raw = [normal.as_slice(), &null, &normal].concat();
        for format in ["fixed", "rdw"] {
            let mut framed = Vec::new();
            if format == "fixed" {
                framed.extend_from_slice(&raw);
            } else {
                for bytes in [&normal, &null, &normal] {
                    framing::write_rdw(&mut framed, bytes, 32760).unwrap();
                }
            }
            let mut config = json!({"format":format,"encoding":"utf-8","fields":fields});
            if format == "fixed" {
                config["fixed"] = json!({"record_separator":"none"});
            }
            let (_dir, mut reader, schema) = test_reader(config, &framed);
            let mut row_reader = InputReader::open(&reader.dataset, &reader.layout).unwrap();
            let batch = reader.next_batch(schema.clone(), Some(7)).unwrap().unwrap();
            assert_eq!(batch.num_rows(), 3);
            for row in 0..3 {
                let mut expected = row_reader.next_row().unwrap().unwrap();
                expected.push(ScalarValue::Utf8(Some(format!(
                    "{:016x}{:016x}",
                    7,
                    row + 1
                ))));
                let actual = batch
                    .columns()
                    .iter()
                    .map(|c| ScalarValue::try_from_array(c, row).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "{format} row {row}");
                if row == 1 {
                    assert!(actual[..actual.len() - 1].iter().all(ScalarValue::is_null));
                }
            }
            assert!(reader.next_batch(schema, Some(7)).unwrap().is_none());
        }
    }

    #[test]
    fn direct_decoding_preserves_text_null_and_encoding_rules() {
        for encoding in ["utf-8", "shift_jis", "utf-16le", "utf-16be", "iso-2022-jp"] {
            let encoding = crate::encoding::TextEncoding::resolve(encoding).unwrap();
            let data = encoding.encode("日本　").unwrap();
            let config = json!({"format":"fixed","encoding":encoding.name(),"fixed":{"record_separator":"none"},"fields":[{"name":"text","length":data.len(),"pad_char":"　"}]});
            let (_dir, mut reader, schema) = test_reader(config, &data);
            let batch = reader.next_batch(schema, Some(1)).unwrap().unwrap();
            assert_eq!(
                ScalarValue::try_from_array(batch.column(0), 0).unwrap(),
                ScalarValue::Utf8(Some("日本".into()))
            );
        }
        let config = json!({"format":"fixed","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":[{"name":"n","type":"decimal","length":8,"precision":6,"scale":2,"nullable":true,"null_if_pattern":"NULL[0-9]+"}]});
        let (_dir, mut reader, schema) = test_reader(config.clone(), b"NULL123   123.45");
        let batch = reader.next_batch(schema, Some(1)).unwrap().unwrap();
        assert!(
            ScalarValue::try_from_array(batch.column(0), 0)
                .unwrap()
                .is_null()
        );
        assert_eq!(
            ScalarValue::try_from_array(batch.column(0), 1).unwrap(),
            ScalarValue::Decimal128(Some(12345), 6, 2)
        );
        let (_dir, mut reader, schema) = test_reader(config, b"  123.45NULLIFY ");
        let error = reader.next_batch(schema, Some(1)).unwrap_err();
        assert_eq!(error.code, 3);
        assert!(error.message.contains("record 2 byte 8"));
        assert!(error.message.contains("column n offset 0 width 8"));
    }

    #[test]
    fn conservative_bound_does_not_reject_large_trimmed_records() {
        let config = json!({"format":"fixed","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":[{"name":"text","length":framing::MAX_RECORD}]});
        let (_dir, mut reader, schema) =
            test_reader(config.clone(), &vec![b' '; framing::MAX_RECORD]);
        let batch = reader.next_batch(schema.clone(), Some(1)).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            ScalarValue::try_from_array(batch.column(0), 0).unwrap(),
            ScalarValue::Utf8(Some(String::new()))
        );
        assert!(reader.next_batch(schema, Some(1)).unwrap().is_none());
        let (_dir, mut reader, schema) = test_reader(config, &vec![b'x'; framing::MAX_RECORD]);
        assert_eq!(reader.next_batch(schema, Some(1)).unwrap_err().code, 3);
    }

    #[test]
    fn read_ahead_record_is_validated_before_returning_a_full_batch() {
        let width = framing::MAX_RECORD / 2;
        let config = json!({"format":"fixed","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":[{"name":"text","length":width-2,"trim":"none"},{"name":"number","length":2,"type":"uzoned","zone":"ascii"}]});
        let mut bytes = vec![b'x'; width * 2];
        bytes[width - 2..width].copy_from_slice(b"12");
        bytes[width * 2 - 2..].copy_from_slice(b"1x");
        let (_dir, mut reader, schema) = test_reader(config, &bytes);
        let error = reader.next_batch(schema, Some(1)).unwrap_err();
        assert_eq!(error.code, 3);
        assert!(error.message.contains("record 2"));
        assert!(error.message.contains("column number"));
    }

    #[test]
    fn order_key_preserves_hex_format_and_boundaries() {
        for dataset in [1, 15, 16, u32::MAX as usize] {
            for record in [0, 15, 16, u32::MAX as u64, u64::MAX] {
                assert_eq!(
                    std::str::from_utf8(&order_key(dataset, record)).unwrap(),
                    format!("{dataset:016x}{record:016x}")
                );
                if record < u64::MAX {
                    assert!(order_key(dataset, record) < order_key(dataset, record + 1));
                }
            }
        }
    }
    #[test]
    fn csv_null_and_empty() {
        let mut r = CsvReader::new(&b"a,,\"\",\"x\ny\"\r\n\n\"a\"\"b\",\n"[..], b',', b'"');
        let row = r.next_record().unwrap().unwrap();
        assert_eq!(
            row[1],
            Cell {
                value: "".into(),
                was_quoted: false
            }
        );
        assert_eq!(
            row[2],
            Cell {
                value: "".into(),
                was_quoted: true
            }
        );
        assert_eq!(row[3].value, "x\ny");
        assert_eq!(r.next_record().unwrap().unwrap().len(), 1);
        assert_eq!(r.next_record().unwrap().unwrap()[0].value, "a\"b");
        assert!(r.next_record().unwrap().is_none());
    }
    #[test]
    fn csv_errors() {
        for data in [&b"\"abc"[..], &b"\"a\"x\n"[..], &b"a\r"[..]] {
            assert!(CsvReader::new(data, b',', b'"').next_record().is_err());
        }
    }
}
