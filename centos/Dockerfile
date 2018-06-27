FROM centos:6.8

# Install all tools needed
RUN yum install -y epel-release && yum update -y && \
    yum install -y cmake gcc-c++ boost-devel libevent2-devel libzip-devel snappy-devel xz-devel lz4-devel tbb-devel gdb

RUN yum groupinstall -y 'Development Tools'
