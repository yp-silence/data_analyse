import os
from utils.logger_utils import get_logger

logg = get_logger()


def clean_file(path, suffix='xlsx'):
    # 文件夹
    base_dir = os.getcwd()
    abs_path = os.path.join(base_dir, path)
    if os.path.isdir(abs_path):
        # 批量删除
        for name in [na for na in os.listdir(abs_path) if na.endswith(suffix)]:
            file_name = os.path.join(base_dir, name)
            os.remove(os.path.join(base_dir, name))
            logg.info(f'delete {file_name} success')
    else:
        # 文件直接删除
        logg.info(f'delete {abs_path} success')
        os.remove(abs_path)
