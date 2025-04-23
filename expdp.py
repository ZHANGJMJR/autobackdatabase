import paramiko


def execute_remote_expdp(hostname, port, username, password, parfile_path):
    try:
        # 创建 SSH 客户端
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # 连接到远程服务器
        ssh_client.connect(hostname=hostname, port=port, username=username, password=password)

        # 构造 expdp 命令
        # 假设需要先加载 Oracle 环境，比如通过 source 一个环境脚本
        # load_oracle_env_command = 'source /path/to/oracle_env.sh;'  # 替换为实际的环境加载脚本路径
        load_oracle_env_command = 'cd /data1/expdumpfile; source ~/.bash_profile ;'
        expdp_command = f'expdp xyjt2022/XYJT2022 parfile={parfile_path}'

        # 组合完整命令
        full_command = f'{load_oracle_env_command} {expdp_command}'

        # 执行命令
        stdin, stdout, stderr = ssh_client.exec_command(full_command)

        # 读取输出
        output = stdout.read().decode()
        error = stderr.read().decode()

        # 打印输出
        print("Output:")
        print(output)
        print("Error:")
        print(error)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # 关闭连接
        ssh_client.close()


# 远程服务器信息
hostname = "192.168.100.60"  # 替换为你的远程服务器地址
port = 22  # 默认 SSH 端口
username = 'oracle' #"root"  # 替换为你的用户名
password = 'oracle' #"Sykj@8472122,./"  # 替换为你的密码

# 参数文件路径
parfile_path = "/data1/expdumpfile/expdp_params.par"  # 替换为你的参数文件路径

# 执行 expdp
execute_remote_expdp(hostname, port, username, password, parfile_path)