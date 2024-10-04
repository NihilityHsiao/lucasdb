use log::error;

use crate::{
    data::{
        data_file::{get_data_file_name, DataFile},
        MERGE_FINISHED_FILE_NAME,
    },
    prelude::*,
};
use std::{fs, path::PathBuf};

pub mod merge;

const MERGE_DIR_NAME: &'static str = "merge";
const MERGE_FIN_KEY: &[u8] = "merge.finished".as_bytes();

/// 用于merge的临时目录
fn get_merge_path(dir_path: PathBuf) -> PathBuf {
    // todo: 删掉unwrap
    let file_name = dir_path.file_name().unwrap();
    let merge_name = format!("{}-{}", file_name.to_str().unwrap(), MERGE_DIR_NAME);

    let parent = dir_path.parent().unwrap();
    parent.to_path_buf().join(merge_name)
}

/// 加载merge数据目录
pub(crate) fn load_merge_files(dir_path: PathBuf) -> Result<()> {
    let merge_path = get_merge_path(dir_path.clone());
    // 没有发生merge
    if !merge_path.is_dir() {
        return Ok(());
    }

    let dir = match fs::read_dir(merge_path.clone()) {
        Ok(dir) => dir,
        Err(e) => {
            error!("failed to read merge directory:{}", e);
            return Err(Errors::IO(e));
        }
    };

    // 查找是否有标识merge完成的文件
    let mut merge_file_names = vec![];
    let mut merge_finished = false;
    for file in dir {
        if let Ok(entry) = file {
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();
            if file_name.ends_with(MERGE_FINISHED_FILE_NAME) {
                merge_finished = true;
            }
            merge_file_names.push(entry.file_name());
        }
    }

    if !merge_finished {
        fs::remove_dir_all(merge_path.clone())?;
        return Ok(());
    }

    // 打开标识merge完成的文件,取出未参与merge的文件id
    let merge_fin_file = DataFile::new_merge_fin_file(merge_path.clone())?;
    let merge_fin_record = merge_fin_file.read_log_record(0)?;

    let v = String::from_utf8(merge_fin_record.record.value).unwrap();
    let non_merge_fid = v.parse::<u32>().unwrap(); // 未参与merge的文件id

    // 已经merge的文件删除
    for fid in 0..non_merge_fid {
        let file = get_data_file_name(&dir_path, fid);
        if !file.is_file() {
            continue;
        }

        fs::remove_file(file)?;
    }

    // 新的数据文件移动到数据库目录
    for file_name in merge_file_names {
        let src_path = merge_path.join(file_name.clone());
        let dst_path = dir_path.join(file_name.clone());
        fs::rename(src_path, dst_path)?;
    }
    fs::remove_dir_all(merge_path.clone())?;

    Ok(())
}
