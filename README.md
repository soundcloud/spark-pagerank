# PageRank in Spark

This is an implementation of PageRank in Spark, using Spark's standard RDD API.

## Performance

We run this on one of our behaviour graphs which consists of approximately 650M vertices and 14B edges. Using the following Spark configuration, and in-memory persistence of edge and vertex RDDs, we obtain iteration times on the order of 3-5m each.

Configuration example:

 - YARN
 - dynamic allocation: no
 - number of executors: 256
 - number of executor cores: 4
 - executor memory: 28G

## Features

- fast iterations
- stopping criteria:
  - number of iterations threshold
  - convergence threshold
- parameterised "teleport" probability
- weighted edges (requires normalized out-edge weights)
- supports "dangling" vertices (no out edges)
- supports arbitrary (e.g. non-uniform) priors (as values in vertices)
- utilities for building, preparing and validating input graphs

## Usage

Include it as a dependency in your sbt project:
`"com.soundcloud" %% "spark-pagerank" % <version>`

## Contact

Please contact [Josh](mailto:josh@soundcloud.com) for more details, or with any questions or comments.
