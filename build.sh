#!/bin/bash
repo_dir="$(cd "$(dirname "$0")"; pwd)"

progress () { echo "====== $@" ; }

progress "Update Submodules RV-Monitor and JavaMOP"
( mkdir -p "$repo_dir/ext" && \
  cd "$repo_dir/ext" && \
  git submodule update --init --recursive )

if [ ! -f "$repo_dir/ext/rv-monitor/target/release/rv-monitor/lib/rv-monitor.jar" ]; then
    progress "Building RV-Monitor (without LLVM)"
    ( cd "$repo_dir/ext/rv-monitor" && \
      mvn clean install -DskipTests -pl '!llvmaop' -pl '!installer' )
fi

if [ ! -f "$repo_dir/ext/javamop/target/javamop-4.0-SNAPSHOT.jar" ]; then
    progress "Building JavaMOP"
    ( cd "$repo_dir/ext/javamop" && \
      mvn clean package -DskipTests )
fi
