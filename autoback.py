import paramiko
import os
from datetime import datetime, timedelta
import time
import subprocess
import logging
import sys
import configparser


#   ssh -o HostKeyAlgorithms=+ssh-rsa -o PubkeyAcceptedAlgorithms=+ssh-rsa root@192.168.100.6
#   ssh  root@192.168.100.60
#   C:\Users\Administrator\.ssh\id_rsa   私钥地址

# 配置日志记录
def setup_logger():
    today = datetime.today().strftime('%Y%m%d')
    log_filename = f"logs/automation_{today}.log"

    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging


# 从文本文件读取server类型
# def read_server_config():
#     config_file = 'server_config.txt'
#     if os.path.exists(config_file):
#         with open(config_file, 'r') as file:
#             server_type = file.readline().strip().lower()
#             if server_type in ['test', 'prod']:
#                 logging.info(f"Server config loaded: {server_type}")
#                 return server_type
#             else:
#                 logging.error(f"Invalid server type in {config_file}: {server_type}")
#                 return None
#     else:
#         logging.error(f"Server config file {config_file} not found.")
#         return None

# 执行SSH命令并记录日志
def execute_ssh_command(ssh_client, command, logger):
    logging.info(f"Executing command: {command}")
    stdin, stdout, stderr = ssh_client.exec_command(command)

    stdout_data = stdout.read().decode()
    stderr_data = stderr.read().decode()

    if stdout_data:
        logging.info(stdout_data)
    if stderr_data:
        logging.error(stderr_data)


# 创建SSH客户端并连接
def create_ssh_client(host, port, username, key_filepath, logger):
    logging.info(f"Connecting to SSH server {host}:{port} as {username}")
    key = paramiko.RSAKey.from_private_key_file(key_filepath)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if host =='192.168.100.6':
        client.connect(host, port=port, username=username, password='tp@kj,./')
    else:
        client.connect(host, port=port, username=username, pkey=key)
    logging.info("SSH connection established.")
    return client


# 创建新的expdp参数文件
def create_expdp_parfile(hostname, username, remote_folder,ssh_client):
    today = datetime.today().strftime('%Y%m%d')
    logging.info(f"Creating new expdp parameter file: expdp_params_{today}.par")

    parfile_content = f"""schemas=XYJT2022
directory=xyback
dumpfile=expdata{today}_1.dmp,expdata{today}_2.dmp,expdata{today}_3.dmp,expdata{today}_4.dmp,expdata{today}_5.dmp
exclude=table:"LIKE 'VT%'"
exclude=table:"LIKE '%LOG'"
exclude=table:"LIKE 'T_WFR_%'"
exclude=table:"IN ('T_ATS_OTTOTAKEWORKDETAIL','T_WFR_ACTINST','T_SSC_ASSIGNFLOWTRACKHIS','T_WFR_ASSIGNDETAIL','T_SSC_ASSIGNFLOWINSTENTRYHIS','T_HR_PERSONPHOTO','T_SSC_ASSIGNFLOWTRACKHIS','T_SSC_ASSIGNPERSONTRACK')"
parallel=5
logfile=expdata{today}.log
cluster=n"""

    with open(f"expdp_params_{today}.par", 'w') as file:
        file.write(parfile_content)
    upload_parfile( hostname, username, remote_folder, f"expdp_params_{today}.par",ssh_client)
    logging.info(f"New expdp parameter file created: expdp_params_{today}.par")

def upload_parfile(hostname, username, remote_file,local_file,ssh_client):
    # ssh_client.exec_command(command)
    command = [
        'scp',
        local_file,
        f'{username}@{hostname}:{remote_file}'
    ]
    # subprocess.run(command, check=True)
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
        ssh_client.exec_command(f"chown oracle:oinstall {remote_file}/{local_file}")
        # print("stdout:", result.stdout.decode())
        # print("stderr:", result.stderr.decode())
        logging.info(f"Downloaded file: {result.stdout.decode()}")
        logging.info(f"Downloaded file: {result.stderr.decode()}")
    except subprocess.TimeoutExpired:
        # print("传输超时，请检查网络连接或文件大小。")
        logging.info("传输超时，请检查网络连接或文件大小。")
    except subprocess.CalledProcessError as e:
        # print(f"传输失败，错误码：{e.returncode}")
        logging.info(f"传输失败，错误码：{e.returncode}")
def scp_download_large_file(hostname, username, remote_file, local_file):
    command = [
        'scp',
        f'{username}@{hostname}:{remote_file}',
        local_file
    ]
    # subprocess.run(command, check=True)
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=21600)
        # print("stdout:", result.stdout.decode())
        # print("stderr:", result.stderr.decode())
        logging.info(f"SCP ：{ result.stdout.decode()}")
        logging.info(f"SCP ：{result.stderr.decode()}")
    except subprocess.TimeoutExpired:
        logging.info("传输超时，请检查网络连接或文件大小。")
    except subprocess.CalledProcessError as e:
        logging.info(f"传输失败，错误码：{e.returncode}")

# 执行expdp命令并下载文件
def export_data(ssh_client, logger,host,username,remote_folder,local_folder):
    today = datetime.today().strftime('%Y%m%d')
    new_parfile_path = f"{remote_folder}expdp_params_{today}.par"
    if host=='192.168.100.6':
        oracle_user='system/xydbora'
    else:
        oracle_user='xyjt2022/XYJT2022'
    create_expdp_parfile(host, username, remote_folder,ssh_client)

    export_command = f"su - oracle -c 'source ~/.bash_profile && cd {remote_folder} && rm expdata*.dmp && expdp {oracle_user} parfile={new_parfile_path}'"
    execute_ssh_command(ssh_client, export_command, logger)

    time.sleep(60)
    # 下载文件
    logging.info(f"Starting file transfer from  {remote_folder}  to {local_folder} directory.")
    sftp_client = ssh_client.open_sftp()

    # 清空本地文件夹
    for filename in os.listdir(local_folder):
        file_path = os.path.join(local_folder, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
            logging.info(f"Deleted local file: {file_path}")

    # 下载文件
    for filename in sftp_client.listdir(remote_folder):
        if filename.endswith('.dmp') or filename.endswith('.log'):
            remote_file = os.path.join(remote_folder, filename)
            local_file = os.path.join(local_folder, filename)
            # sftp_client.get(remote_file, local_file)
            scp_download_large_file(
                hostname=host,
                username=username,
                remote_file=remote_file,
                local_file=local_file
            )
            logging.info(f"Downloaded file: {filename}")

    sftp_client.close()


# 执行Docker命令
def execute_docker_commands(logger):
    logging.info("Restarting Docker container 'oracle11foreas'.")
    subprocess.run(["docker", "restart", "oracle11foreas"], check=True)

    # 等待25秒
    time.sleep(25)

    # 执行docker exec命令
    logging.info("Executing Docker command to run autoimp.sh.")
    result =subprocess.run(["docker", "exec", "oracle11foreas", "bash", "-c", "/home/oracle/autoimp.sh"],
                   stdout=subprocess.PIPE,  # 捕获标准输出
                   stderr=subprocess.STDOUT,  # 合并标准错误到标准输出
                   check=True)
    logging.info("autoimp.sh script executed.")
    logging.info("autoimp.sh executed info #######    \n%s",result.stdout)

# 定义主工作函数
def job(logger):
    # server= read_server_config()  # 取环境
    config = configparser.ConfigParser()
    config.read('config.ini')
    server =config.get('Settings', 'server')
    if server=='test':
        host = '192.168.100.60'
        remote_folder = '/data1/expdumpfile/'
    elif server=='prod':
        host = '192.168.100.6'
        remote_folder = '/db/base/xyback/'
    else:
        host = ''
        remote_folder = '/tmp'
        logger.info("Program executed without host.")
        sys.exit(0)  # 成功退出，返回码为 0
    port = 22
    username = 'root'
    key_filepath = r'C:\Users\Administrator\.ssh\id_rsa'
    local_folder = r'F:\DockerData\oracle11g\expdata'

    ssh_client = create_ssh_client(host, port, username, key_filepath, logger)
    export_data(ssh_client, logger,host, username,remote_folder,local_folder)
    execute_docker_commands(logger)

    ssh_client.close()

#
# # 从TXT文件读取执行日期
# def read_execution_date():
#     date_file = 'execution_date.txt'  # 日期文件路径  如 2025-04-21 23:00
#     if os.path.exists(date_file):
#         with open(date_file, 'r') as file:
#             date_str = file.readline().strip()
#             logging.info(f"Scheduled execution date: {date_str}")
#             try:
#                 execution_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
#                 return execution_time
#             except ValueError:
#                 logging.error(f"Invalid date format in {date_file}. Please use 'YYYY-MM-DD HH:MM'.")
#                 return None
#     else:
#         logging.error(f"Date file {date_file} not found.")
#         return None
#

# 等待直到指定的执行时间
def schedule_job(logger):
    # execution_time = read_execution_date()
    config = configparser.ConfigParser()
    config.read('config.ini')
    # server = config.get('Settings', 'exedatetime')
    try:
        execution_time = datetime.strptime(config.get('Settings', 'exedatetime'),
                                           "%Y-%m-%d %H:%M")
    except ValueError:
        logging.error(f"Invalid date format in exedatetime. Please use 'YYYY-MM-DD HH:MM'.")
        sys.exit(0)
    if execution_time:
        logging.info(f"Waiting until {execution_time} to execute the job.")

        # 等待直到指定的执行时间
        while True:
            now = datetime.now()
            if now >= execution_time:
                job(logger)  # 执行任务
                logging.info(f"Job executed at: {datetime.now()}")
                break  # 执行完后退出
            time.sleep(30)  # 每30秒检查一次


# 启动定时任务
def setup_and_run():
    logger = setup_logger()
    schedule_job(logger)


setup_and_run()
