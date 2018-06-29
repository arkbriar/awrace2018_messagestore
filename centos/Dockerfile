FROM centos:6.8

# Add yum source and setup experiment environment
RUN wget http://people.centos.org/tru/devtools-2/devtools-2.repo -O /etc/yum.repos.d/devtools-2.repo
RUN yum install -y epel-release && yum update -y && \
    yum install devtoolset-2-gcc devtoolset-2-binutils devtoolset-2-gcc-c++ \
    yum install -y cmake3 boost-devel libevent2-devel libzip-devel snappy-devel xz-devel lz4-devel tbb-devel

RUN yum groupinstall -y 'Development Tools'
