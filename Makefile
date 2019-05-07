all: build

.PHONY: clean
clean:
	./sbt clean

.PHONY: distclean
distclean:
	rm -rf \
    .deps \
    project/project project/target \
    target

.PHONY: build
build: package

.PHONY: package
package:
	./sbt assembly doc

.PHONY: test
test:
	./sbt ";+ test"

.PHONY: publish
publish:
	./sbt ";+ publish"

.PHONY: release
release:
	./sbt ";+ release"
