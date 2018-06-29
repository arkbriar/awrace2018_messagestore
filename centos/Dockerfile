FROM centos:6.8

# Add yum source and setup experiment environment
RUN yum install -y wget
RUN wget http://people.centos.org/tru/devtools-2/devtools-2.repo -O /etc/yum.repos.d/devtools-2.repo
RUN yum install -y epel-release && yum update -y && \
    yum install devtoolset-2-gcc devtoolset-2-binutils devtoolset-2-gcc-c++ \
    yum install -y cmake3 boost-devel libevent2-devel libzip-devel snappy-devel xz-devel lz4-devel

# Get and build tbb
ENV LD_LIBRARY_PATH=/opt/rh/devtoolset-2/root/usr/lib64:$LD_LIBRARY_PATH
ENV PATH=/opt/rh/devtoolset-2/root/usr/bin:$PATH
RUN wget https://github.com/01org/tbb/archive/2018_U5.tar.gz -O /tmp/2018_U5.tar.gz \
    && cd /tmp && tar xf 2018_U5.tar.gz && cd tbb-2018_U5 \
    && make -j4 \
    && cp $(find . -name "*.so*" -o -name "") /usr/lib64/ && cp -R include/tbb /usr/include/

# Setup cmake3
RUN ln -sf /usr/bin/cmake3 /usr/bin/cmake
