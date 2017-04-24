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
	./sbt assembly

.PHONY: test
test:
	./sbt test
