use std::{hint::black_box, time::Instant};
use parquet_variant_compute::{VariantArray, VariantArrayBuilder};
use parquet_variant_json::{JsonToVariant, VariantToJson};

fn main() {
    const ROWS: usize = 1024;
    const BATCHES: usize = 256;
    let mut builder = VariantArrayBuilder::new(ROWS);
    for row in 0..ROWS {
        builder.append_json(&format!(r#"{{"http":{{"method":"GET","status":200}},"path":"/events/{row}","tags":["api","production"],"duration":12345}}"#)).unwrap();
    }
    let source = builder.build();
    for trial in 0..4 {
        for per_row in if trial % 2 == 0 { [true, false] } else { [false, true] } {
            let started = Instant::now();
            let mut bytes = 0usize;
            for _ in 0..BATCHES {
                let prepared = (!per_row).then(|| VariantArray::try_new(black_box(source.inner())).unwrap());
                for row in 0..ROWS {
                    let owned;
                    let array = if let Some(array) = &prepared { array } else {
                        owned = VariantArray::try_new(black_box(source.inner())).unwrap();
                        &owned
                    };
                    bytes += black_box(array.value(row).to_json_string().unwrap()).len();
                }
            }
            println!("trial={trial} per_row={per_row} rows={} bytes={bytes} elapsed_ms={:.3}", ROWS * BATCHES, started.elapsed().as_secs_f64() * 1000.0);
        }
    }
}
