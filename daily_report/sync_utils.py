"""
file sync utils
"""

import os
import click

from utils.logger_utils import get_logger


logg = get_logger()

BASE_DIR = '/opt/ljhy-report/'
ROOT_DIR = os.getcwd()


def upload_file_to_remote(path):
    # 将本地文件发送到远程服务器
    file_path = os.path.join(ROOT_DIR, path)
    logg.info(f'file_path:{file_path}')
    if os.path.isdir(path):
        cmd = f'scp -r {file_path} root@192.168.22.61:{BASE_DIR} '
    else:
        cmd = f'scp  {file_path} root@192.168.22.61:{BASE_DIR} '
    logg.info(f'cmd:{cmd}')
    code = os.system(cmd)
    logg.info(f'code:{code}')
    if code == 0:
        logg.info(f'文件夹{path} 发送到{path}成功')
    else:
        logg.error(f'文件夹发送失败')


def download_file_to_local(path, local_path='~'):
    """
    从远程服务器下载文件到本地
    :param path:
    :param local_path:
    :return:
    """
    file_path = os.path.join(ROOT_DIR, path)
    """
    scp root@192.168.1.2:/home/a.txt /home/xue/
    """
    cmd = f"scp root@192.168.22.61:{file_path} {local_path}"
    code = os.system(cmd)
    if code == 0:
        logg.info(f'远程服务器文件{path} 发送到本地{path}成功')
    else:
        logg.error(f'文件夹发送失败')


@click.command()
@click.option('--path', default='../../data_analyse', help='待上传代码路径')
@click.option('--action', default='sever', type=click.Choice(['sever','local']), help='上传的服务器还是下载到本地')
@click.option('--local_path', default='~', help='本地存储路径')
def run(action, path, local_path):
    if action == 'sever':
        upload_file_to_remote(path)
    else:
        download_file_to_local(path,local_path)
    path = '../../data_analyse'
    upload_file_to_remote(path)


if __name__ == '__main__':
    run()
