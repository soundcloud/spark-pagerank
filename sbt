#!/bin/sh

# everything is relative to this script
cd $(dirname "$0")

VERSION="0.12.4"
REMOTE_ZIP="http://scalasbt.artifactoryonline.com/scalasbt/sbt-native-packages/org/scala-sbt/sbt/$VERSION/sbt.zip"
ZIP="target/sbt-launch-$VERSION.zip"
JAR=".deps/build/sbt/sbt-launch-$VERSION.jar"

# download launcher
if [ ! -f $JAR ]; then
  echo "[INFO] Downloading sbt launcher binary"
  mkdir -p target
  mkdir -p .deps/build/sbt
  curl -o $ZIP $REMOTE_ZIP
  unzip $ZIP -d target
  mv target/sbt/bin/sbt-launch.jar $JAR
  rm -rf target/sbt $ZIP
fi

java \
  -Dsbt.version=$VERSION \
  -Dsbt.ivy.home=.deps/build/ivy2 \
  -Dsbt.global.base=.deps/build/sbt \
  -Dsbt.boot.directory=.deps/build/sbt/boot \
  -Xmx1G \
  -Xms1G \
  -XX:MaxPermSize=1G \
  -jar $JAR "$@"
