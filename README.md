# PageRank in Spark

This is an implementation of PageRank in Spark, using Spark's standard RDD API.

## Performance

We run this on one of our behaviour graphs which consists of approximately 650M vertices and 14B edges. Using the following Spark configuration, and in-memory persistence of edge and vertex RDDs, we obtain iteration times on the order of 3-5m each.

Configuration example:

 - YARN
 - Dynamic allocation: no
 - Number of executors: 256
 - Number of executor cores: 4
 - Executor memory: 28G

## Features

- Fast iterations
- Stopping criteria:
  - Number of iterations threshold
  - Convergence threshold
- Parameterised "teleport" probability
- Weighted edges (requires normalized out-edge weights)
- Supports "dangling" vertices (no out edges)
- Supports arbitrary (e.g. non-uniform) priors (as values in vertices)
- Utilities for building, preparing and validating input graphs

## Usage

Include it as a dependency in your sbt project:
`"com.soundcloud" %% "spark-pagerank" % <version>`

## Performance Tuning

- Persist the edges and vertices of the graph with replicas: `StorageLevel.MEMORY_AND_DISK_2`
- Enable Kryo serialization: `KryoSerialization.useKryo`

## Contact

Please contact [Josh](mailto:josh@soundcloud.com) for more details, or with any questions or comments.
