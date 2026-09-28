#!/bin/bash
# runs in an i386 container: a perl with 64-bit time_t in its ccflags, as on
# Debian armhf and armel since perl 5.40
set -eu
export DEBIAN_FRONTEND=noninteractive
apt-get -qq update
apt-get -qq install -y --no-install-recommends gcc make libc6-dev libssl-dev \
    redis-server curl ca-certificates >/dev/null

curl -fsSL https://www.cpan.org/src/5.0/perl-5.40.2.tar.gz | tar --no-same-owner -xz -C /tmp
cd /tmp/perl-5.40.2
sh Configure -des -Dprefix=/opt/perl -Dman1dir=none -Dman3dir=none \
    -Accflags='-D_TIME_BITS=64 -D_FILE_OFFSET_BITS=64' >/dev/null
make -j"$(nproc)" >/dev/null
make install >/dev/null
/opt/perl/bin/perl -V:ivsize -V:ccflags

curl -fsSL https://cpanmin.us | /opt/perl/bin/perl - -q --notest \
    EV File::Which Test::RedisServer Test::Deep Test::TCP Devel::Refcount

cp -a /src /tmp/build
cd /tmp/build
/opt/perl/bin/perl Makefile.PL
make
/opt/perl/bin/prove -b t/
