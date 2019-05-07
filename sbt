#!/bin/sh

# Note: The version numnber here should always match what is in
#       project/build.properties since that is what Travis CI uses.
VERSION="0.13.18"
REMOTE_TGZ="https://piccolo.link/sbt-${VERSION}.tgz"

SCRIPT_DIR=$(dirname "$0")
TMP_DIR="$SCRIPT_DIR/.deps/tmp"
SBT_DIR="$SCRIPT_DIR/.deps/build/sbt"
IVY2_DIR="$SCRIPT_DIR/.deps/build/ivy2"
JAR="$SBT_DIR/sbt-launch-$VERSION.jar"

# download launcher
if [ ! -f $JAR ]; then
  echo "[INFO] Downloading sbt launcher binary: ${REMOTE_TGZ}"
  mkdir -p $TMP_DIR
  mkdir -p $SBT_DIR
  curl -Lk $REMOTE_TGZ | tar xz -C $TMP_DIR
  mv $TMP_DIR/sbt/bin/sbt-launch.jar $JAR
  rm -rf $TMP_DIR
fi

java \
  -Dsbt.version=$VERSION \
  -Dsbt.global.base=$SBT_DIR/base/$VERSION \
  -Dsbt.boot.directory=$SBT_DIR/boot \
  -Dsbt.ivy.home=$IVY2_DIR \
  -Xmx2G \
  -Xms2G \
  -jar $JAR "$@"
