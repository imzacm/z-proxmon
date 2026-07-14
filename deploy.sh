#!/usr/bin/env bash

set -Eeuo pipefail

export CC=clang
export CXX=clang
export AR=llvm-ar
export RUSTFLAGS="-Clink-arg=-fuse-ld=lld -C target-cpu=znver2"

cargo build --release

ssh root@192.168.0.127 'systemctl stop z-proxmon'

# scp "z-proxmon.service" root@192.168.0.127:/etc/systemd/system/
# ssh root@192.168.0.127 'systemctl daemon-reload'

scp "target/release/z-proxmon" root@192.168.0.127:/mnt/media/bin/
ssh root@192.168.0.127 'systemctl start z-proxmon && journalctl -u z-proxmon -b --follow'
