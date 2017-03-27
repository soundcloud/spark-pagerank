all: test package

.PHONY: clean
clean:
	./sbt clean

.PHONY: distclean
distclean:
	rm -rf project/target project/build

.PHONY: package
package:
	./sbt package

.PHONY: test
test:
	./sbt test
