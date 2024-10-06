use std::{fs, path::PathBuf};

pub fn create_dir_if_not_exist(path: &PathBuf) -> Result<(), std::io::Error> {
    if !path.is_dir() {
        fs::create_dir_all(path)?
    }

    Ok(())
}

/// 获取磁盘剩余空间, 单位 bytes
pub fn available_disk_size() -> u64 {
    if let Ok(size) = fs2::available_space(PathBuf::from("/")) {
        return size;
    }
    0
}

/// 获取磁盘数据目录大小
pub fn dir_disk_size(dir_path: &PathBuf) -> u64 {
    if let Ok(size) = fs_extra::dir::get_size(dir_path) {
        return size;
    }
    0
}

/// 将`src`目录的内容拷贝到`dest`
/// 如果`src`中的路径以`exclue`结尾,就忽略
/// 如果`dest`不存在,就会创建目录
/// 注意: 该方法是递归调用
pub fn copy_dir(src: PathBuf, dest: PathBuf, exclude: &[&str]) -> std::io::Result<()> {
    if !dest.exists() {
        fs::create_dir_all(&dest)?;
    }

    for dir_entry in fs::read_dir(src)? {
        let entry = dir_entry?;
        let src_path = entry.path();
        if exclude.iter().any(|&x| src_path.ends_with(x)) {
            continue;
        }

        let dest_path = dest.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(src_path, dest_path, exclude)?;
        } else {
            fs::copy(src_path, dest_path)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_available_disk_size() {
        let size = available_disk_size();
        assert_ne!(0, size);
        println!("available_disk_size: {:?}", size);
    }
}
