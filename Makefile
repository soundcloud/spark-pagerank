SBT=crun -i sbt -- sbt

.PHONY: all
all: build

.PHONY: build
build: test
	$(SBT) package

.PHONY: test
test:
	$(SBT) test

.PHONY: clean
clean:
	$(SBT) clean

.PHONY: distclean
distclean:
	rm -rf project/target project/build
