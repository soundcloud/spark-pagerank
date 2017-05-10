all: package

.PHONY: clean
clean:
	./sbt clean

.PHONY: distclean
distclean:
	rm -rf .deps
	rm -rf project/project project/target target

.PHONY: package
package:
	./sbt package

.PHONY: test
test:
	./sbt ";+ test"

.PHONY: publish
publish:
	./sbt ";+ publish"

.PHONY: release
release:
	./sbt ";+ release"
