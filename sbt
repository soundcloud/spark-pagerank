#!/bin/sh

VERSION="0.13.16"
REMOTE_TGZ="https://cocl.us/sbt-${VERSION}.tgz"

SCRIPT_DIR=$(dirname "$0")
SBT_DIR="$SCRIPT_DIR/.deps/build/sbt"
IVY2_DIR="$SCRIPT_DIR/.deps/build/ivy2"
JAR="$SBT_DIR/sbt-launch-$VERSION.jar"

TMP_DIR="$SCRIPT_DIR/.deps/tmp"

# download launcher
if [ ! -f $JAR ]; then
  echo "[INFO] Downloading sbt launcher binary"
  mkdir -p $TMP_DIR
  mkdir -p $SBT_DIR
  curl -Lk $REMOTE_TGZ | tar xz -C $TMP_DIR
  mv $TMP_DIR/sbt/bin/sbt-launch.jar $JAR
  rm -rf $TMP_DIR
fi

java \
  -Dsbt.version=$VERSION \
  -Dsbt.global.base= \
  -Dsbt.boot.directory=$SBT_DIR/boot \
  -Dsbt.ivy.home=$IVY2_DIR \
  -Xmx2G \
  -Xms2G \
  -jar $JAR "$@"
