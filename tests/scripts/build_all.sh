#!/bin/bash

declare -a targets=(
  "aarch64-unknown-linux-gnu"
  "aarch64-unknown-linux-musl"
  "x86_64-unknown-linux-gnu"
  "x86_64-unknown-linux-musl"
)

for target in "${targets[@]}"
do
  echo "Building for $target"
  cross build --release --bin redis-counter-exporter --target $target
done

mkdir tests/tmp/releases
for target in "${targets[@]}"
do
  cp "target/$target/release/redis-counter-exporter" "tests/tmp/releases/redis-counter-exporter-$target"
done

echo "Finished building."