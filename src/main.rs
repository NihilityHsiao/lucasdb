use bytes::{Bytes, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

fn main() {
    println!(
        "length_delimiter_len of u32 max: {}",
        length_delimiter_len(std::u32::MAX as usize)
    );

    println!(
        "length_delimiter_len of u32 min: {}",
        length_delimiter_len(std::u32::MIN as usize)
    );
}
