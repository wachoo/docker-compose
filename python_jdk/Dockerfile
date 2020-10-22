# VERSION 1.0.0
# AUTHOR: Wachoo
# DESCRIPTION: The project is a python app which contains a java jar lib.
FROM centos:7
MAINTAINER wachoo <wachoo@126.com>

COPY ./requirements.txt /usr/local/src/requirements.txt
# update software resource
RUN yum -y install wget && \
    cd /etc/yum.repos.d/  && \
    mkdir bak  && \
    mv *.repo bak  && \
    wget http://mirrors.aliyun.com/repo/Centos-7.repo  && \
    wget http://mirrors.aliyun.com/repo/epel-7.repo  && \
    wget -O /etc/yum.repos.d/CentOS-Base.repo https://mirrors.aliyun.com/repo/Centos-7.repo  && \
    sed -i -e '/mirrors.cloud.aliyuncs.com/d' -e '/mirrors.aliyuncs.com/d' /etc/yum.repos.d/CentOS-Base.repo  && \
    yum -y install zlib-devel bzip2-devel openssl-devel openssl-static ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel libffi-devel lzma gcc  && \
    yum -y groupinstall "Development tools"  && \
#    yum -y install java-1.8.0-openjdk.x86_64  && \
    yum -y install java-1.8.0-openjdk-devel.x86_64  && \
    cd /usr/local/src/  && \
    wget https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tar.xz  && \
    tar xvf Python-3.7.0.tar.xz  && \
    mv Python-3.7.0 /usr/local/python-3.7 && \
    cd /usr/local/python-3.7/ && \
    ./configure --prefix=/usr/local/sbin/python-3.7 && \
    make && make install && \
    rm -rf /usr/bin/python && \
    ln -sv /usr/local/sbin/python-3.7/bin/python3 /usr/bin/python && \
    ln -sv /usr/local/sbin/python-3.7/bin/python3 /usr/bin/python3 && \
    ln -sv /usr/local/sbin/python-3.7/bin/python3 /usr/bin/python37 && \
    sed -i 1s/python/python2.7/ /usr/bin/yum && \
    sed -i 1s/python/python2.7/ /usr/libexec/urlgrabber-ext-down && \
    sed -i 1s/python/python2.7/ /usr/bin/yum-config-manager && \

    sed -i '$a\PYTHON_HOME=/usr/local/sbin/python-3.7' /etc/profile && \
#    sed -i '$a\JAVA_HOME=/usr/lib/jvm/jre-1.8.0-openjdk.x86_64' /etc/profile && \
    sed -i '$a\JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.151-1.b12.el7_4.x86_64' /etc/profile && \
    sed -i '$a\PATH=$PATH:$JAVA_HOME/bin:$PYTHON_HOME/bin' /etc/profile && \
    sed -i '$a\CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar' /etc/profile && \
    sed -i '$a\export PYTHON_HOME JAVA_HOME CLASSPATH PATH' /etc/profile && \
    source /etc/profile && \
    ln -s /usr/local/sbin/python-3.7/bin/pip3 /usr/bin/pip && \
    pip install --upgrade pip

RUN    pip install -r /usr/local/src/requirements.txt