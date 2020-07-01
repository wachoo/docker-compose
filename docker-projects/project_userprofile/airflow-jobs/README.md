# 选人平台ETL作业
## 环境要求
Linux: CentOS 7.4+ or Ubuntu 18+

Mac: 10.13+

Windows 10 1803+

## 安装Docker (Mac版)
```
brew install docker
brew install docker-compose
brew install xhyve
brew install docker-machine
brew install docker-machine-driver-xhyve
brew install docker-machine-nfs

docker-machine create --driver xhyve
eval $(docker-machine env)

docker-machine-nfs default

docker info
```

## Python环境
Python版本：3.6(默认) 或 3.7
Mac安装:
```
brew install python
brew install pipenv
```

CentOS:
```
sudo yum install -y https://centos7.iuscommunity.org/ius-release.rpm
sudo yum install -y python36u python36u-pip python36u-devel
sudo pip3.6 install --upgrade pip
pip install pipenv --user
```

## 项目代码
推荐使用PyCharm打开本项目目录，然后需要初始化pipenv环境。

Pipfile里的Python版本是>=3.6, 需要Python版本是3.6或更新的，也可以修改文件写死一个版本.
初始化pipenv环境，可以在shell里输入
```
export SLUGIFY_USES_TEXT_UNIDECODE=yes
pipenv sync
```
如果以上命令报错，可以尝试下面这种方法
```
export SLUGIFY_USES_TEXT_UNIDECODE=yes
pipenv install 'apache-airflow[hive]'
```
之后就可以在PyCharm里把dags目录添加成Source Folders就可以正常编译Airflow DAG代码了

## Hive环境
1. 使用Docker Hive环境，然后
   上传elasticsearch for hadoop(目录data-dir下的jar文件)到HDFS:///tmp/elasticsearch-hadoop-6.4.2.jar
2. 再将数据project_userprofile/docker-hive/data-dir/的crm.zip解压到Docker Hive项目data-dir/目录下,然后进入hive-server容器，
   将/opt/data-dir/下的文件拷贝到HDFS:///data/目录下的子目录, 具体子目录参考project_userprofile/docker-hive/data-dir/create-tables.sql
3. 使用beeline -u jdbc:hive2://localhost:10000 -u hive -p hive登录beeline
4. 在beeline里用create-tables.sql初始化表

## Airflow Docker Compose环境
1. 确保docker和docker-compose已经安装好
2. 进入airflow-jobs目录，使用docker-compose up -d启动airflow容器
3. 过一会后浏览器里打开http://<docker-machine-ip>:8080, 可以打开airflow界面

## 配置Airflow
1. 浏览器里打开http://<docker-machine-ip>:8080, 在UI里修改Connection的属性
连接名: hive_cli_default
Host: <docker-machine-ip>
Port: 10000
Extra: {"use_beeline":true,"auth":"none"}

连接名: hiveserver2_default
Host: <docker-machine-ip>
Port: 10000

两个连接的用户名和密码都填hive

2. 新建es_default的连接
连接名Conn Id: es_default
Conn Type: HTTP
Host: es服务域名
Port: 9200

3. 要连接 Hive Server 需要额外的安装, 先以root用户进入Airflow容器
```
docker-compose exec -u root webserver bash
apt update
apt-get install libsasl2-modules libsasl2-dev vim
pip install sasl thrift-sasl
pip install six==1.13.0

vi /usr/lib/hive/bin/hive
修改SKIP_HADOOPVERSION=true    :wq保存
exit
```

4. 修改dags/crm_user_profile_dag.py代码中es-server所表示的ES服务器域名或IP

5. 打开user_tag_etl_dag开关

6. 点击dag运行按钮，启动DAG
