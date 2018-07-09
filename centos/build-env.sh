#!/bin/bash

yum install -y wget gcc-c++ cmake3 boost-devel libevent-devel libzip-devel snappy-devel xz-devel lz4-devel gperftools-libs git gdb
ln -sf /usr/bin/cmake3 /usr/bin/cmake
ln -sf /usr/lib64/libtcmalloc.so.4 /usr/lib64/libtcmalloc.so

wget https://github.com/01org/tbb/archive/2018_U5.tar.gz -O /tmp/2018_U5.tar.gz \
    && cd /tmp && tar xf 2018_U5.tar.gz && cd tbb-2018_U5 \
    && make -j4 \
    && cp $(find . -name "*.so*" -o -name "") /usr/lib64/ && cp -R include/tbb /usr/include/

echo '-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAoE0hq5hk0hginFduleTZnmRqhAyMn/PrQxQu23bPIxCjCbcb
HMN0OvqKQpyq9B2BDLz2DRfvViP0/mA2TJrNgv6Od/M35KxgaAohCfELdbypeW4y
LOGSQ5+CIK9tTDIbOtlszJ/nqKIPO9XjYL5lDtjF7Ic1w4PZ92/VmX+q3m83i47W
AWbzkj+imZW2gxcXHUCRgycxdGVIqCvHJBJFJbQgiFJmCTJGhmYbrKLszlMI1hE1
uj3krPSsvjNWxTKVIufGSo2/DYwqqnulwWKw68tGBFJrOcjjgRF0TVvVrrUIAD1k
ewL+Km2egBuSK3Wyjxj7zzBGs2MGH9GZOuMUuQIDAQABAoIBACXOq09ObREEMj2P
fXyK8iyiYGDumMgs/mLewsJuPiJ2DQ4Airt4+dPjPXLCgqt6vfIkKyAhcghuCJ1q
UcQ1ip0HrtBpo9giM8a2BN7UZDCLo6kJLXpaZWXfnBBbb7rV4og5uah1WmzydAdX
xf4OScV7qQrFRQ/s7B6o2McQTluRnghqsWZy9+iWkyeeyNcpgrqcS06Agyo6nRJG
MccrJq/ku55b3QiB3bUSin/iEaG08kXXDkb3dfy9rXmFImLmhAd2bmVxBNeuETnA
SrPcsSBGPhSi6RNSuA/f9wAM18I9lQQ/EwpVneVkCa8MKJKEjiz1UGD3rZonRVJh
gMeqlXUCgYEAywSeHhlCbT8CWKiTKCSYwjHbplUAJnJBURWN2mMSdrustuAN/e+H
L3oxR/LG6j+4m6DZFSvvTxmtcIjDNf1geRCVgqboj9rIsNtP/VZbCCVFyMk/JWge
R5OOKGt+JnE4N5ndBHC18v6DJuV+mSuVguN9+e7TT3+msMVGr663GI8CgYEAyiKp
VsBWgMz+65nDEOmK+GsWYfeEaLAQATrzDZLsuqvjrgpjuBKIH7orl650jgNwaLCi
Z3DyXg5OkZD/56yHLxoMV2qdenHd2B3hquZcdRG5Rd2h2AZvxjNwAPBLS6aq6yrE
nuL0g/mmrWmohdDN36aDSOxNr5tmWhAmECrhUjcCgYBg8P2HpcwgWlwGdch+/Kqs
4B3gWOpPcXNbAo3P4Erqw0b/tBE6VmwY8aIilv0A0hiWx8Gg6G/HN52oBMYiO7Eb
Xh3mW0jlmDUIrlCNyfSB+TpPXDl45TTAPOc+ycVdGeccNa5h9mgLdhiuNBu1jChC
bdqzw6THa07vTeQo86aeZQKBgHJLcRcFgKQpxcbwVbU9aQAYhTp125/aVeJsM7ys
JEFVKlNhuor9p4zRFw8YbP1UAn6fyeTVn6YBTvnOVHTqQDIo92uCdHMk6XznQJAC
26qyj+FqdbYRxaf7s8xEEz8+iYyZzT3ONNZfFpkdklXAAHkCzV8xhqfSJeitEui3
Zlq3AoGBAMY2TfHbbRdU0S7kbkJ1u/FjkTqzE2GxnRERyAn9F7ZoElWyBiPpXRmc
D5Cda5UnbFRmgl5gAsN9WLR8+11E41okowkaxhbQ3ctX1/PTkn7fbstYbxt2kE6l
q+00EN2sBVi9iEr42pYMNv2gXInSiBpwRbkunmvuhRS+AanokG1H
-----END RSA PRIVATE KEY-----' > ~/.ssh/id_rsa

chmod 600 ~/.ssh/id_rsa

yum install -y htop dstat