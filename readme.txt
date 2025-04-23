pyinstaller --clean --onefile --noconsole --icon=logo.ico --add-data "logo.ico;." autoback.py

说明：执行远程Linux Oracle服务器上的具体目录DMP文件删除，然后执行expdp导出数据，
           将导出的DMP文件复制到本地机器上，然后重启docker oracle容器，再执行impdp。


