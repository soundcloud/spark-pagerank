all: test package

.PHONY: clean
clean:
	./sbt clean

.PHONY: distclean
distclean:
	rm -rf .deps .ivy2 .sbt
	rm -rf project/project project/target target

.PHONY: package
package:
	./sbt package

.PHONY: test
test:
	./sbt test
