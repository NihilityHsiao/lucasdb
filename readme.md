# 关于数据的编码格式
每对`key-value`都是一个`LogRecord`,编码格式如下：

| type | key size     | value size   | key | value | crc 校验值 |
| ---- | ------------ | ------------ | --- | ----- | ------- |
| 1 字节 | 变长 (最大 5 字节) | 变长 (最大 5 字节) | 变长  | 变长    | 4 字节    |


# benches
```bash
cargo bench
```