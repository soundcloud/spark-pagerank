[![buildstatus](https://travis-ci.org/soundcloud/spark-pagerank.svg?branch=master)](https://travis-ci.org/soundcloud/spark-pagerank)

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

## Contributing

We welcome contributions by [pull requests](https://github.com/soundcloud/spark-pagerank/pulls) and bug reports or feature requests as [issues](https://github.com/soundcloud/spark-pagerank/issues).

## Publishing and Releasing

To publish an artefact to the Sonatype/Maven central repository (a snapshot or release), you need to have a Sonatype account, PGP keys and sbt plugins setup. Please follow the [sbt guideline](http://www.scala-sbt.org/release/docs/Using-Sonatype.html) for a complete getting started guide. Once this is done, you can use the [sbt-release](https://github.com/sbt/sbt-release) plugin via the `Makefile` to publish snapshots and perform releases.

### Publishing Snapshots

At any point in the development lifecycle, a snapshot can be published to the central repository.

```
make publish
```

### Performing a Release

Once development of a version is complete, the artefact should be released to the central repository. This is a two stage process with the artefact first entering a staging repository, followed by a manual promotion process.

```
make release
```

After a release to the staging repository, the [staging-to-release promotion process](http://central.sonatype.org/pages/releasing-the-deployment.html) must to be followed manually before the artefact is available in the central repository.

## Contact

Please contact [Josh Devins](mailto:josh@soundcloud.com) for more details, or with any questions or comments.

## Copyright

Copyright (c) 2017 [SoundCloud Ltd.](http://soundcloud.com).

See the [LICENSE](LICENSE) file for details.
