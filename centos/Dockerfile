FROM centos:6.8

# Add yum source and setup experiment environment
RUN wget http://people.centos.org/tru/devtools-2/devtools-2.repo -O /etc/yum.repos.d/devtools-2.repo
RUN wget https://github.com/01org/tbb/releases/download/2018_U5/tbb2018_20180618oss_lin.tgz -O /tmp/tbb2018_20180618oss_lin.tgz
RUN yum install -y epel-release && yum update -y && \
    yum install devtoolset-2-gcc devtoolset-2-binutils devtoolset-2-gcc-c++ \
    yum install -y cmake3 boost-devel libevent2-devel libzip-devel snappy-devel xz-devel lz4-devel tbb-devel
