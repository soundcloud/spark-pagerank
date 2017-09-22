[![buildstatus](https://travis-ci.org/soundcloud/spark-pagerank.svg?branch=master)](https://travis-ci.org/soundcloud/spark-pagerank)

# PageRank in Spark

This is an implementation of PageRank in Spark, using Spark's standard RDD API.

## Features

- Fast iterations
- Parameterised "teleport" probability
- Weighted edges (out-edge weights must be normalised)
- Supports "dangling" vertices (no out-edges from a node)
- Supports arbitrary (e.g. non-uniform) priors (as vertex values)
- Various stopping criteria:
  - Number of iterations threshold
  - Convergence threshold (requires additional computation after each iteration)
- Utilities for building, preparing and validating input graphs (incl. out-edge normalisation)

## Usage

Include it as a dependency in your sbt project:
`"com.soundcloud" %% "spark-pagerank" % <version>`

### As A Library

You can use this as a library and call it from within your own drivers. You will want to do this when you have some data preparation to do that does not conform with the built-in driver data interfaces.

More examples of usage as a library can be found in the source of the built-in drivers (see below).

### As Drivers

We include several built-in drivers that operate on plain-text TSV input of labelled edges as a starting point. You will prepare the graph and run PageRank in the following sequence of drivers. Use `--help` to see the arguments and usage of each driver.

1. `com.soundcloud.spark.pagerank.GraphBuilderApp`: Builds a PageRank graph from (non-normalized) weighted edges in TSV format (source, destination, weight), saving the resulting graph (edges and vertices) in Parquet files in preparation for next steps.
1. `com.soundcloud.spark.pagerank.PageRankApp`: Runs PageRank on the graph produced using the functions in `PageRankGraph` or by using the `GraphBuilderApp`.
1. `com.soundcloud.spark.pagerank.ConvergenceCheckApp`: Compares two PageRank vectors and lets the user determine if there is convergence by oututting the sum of the component-wise difference of the vectors. Note that this is an optional tool that is mostly used for debugging. If the user is concerned with iterating until convergence, the user can specify the convergence threshold at runtime to PageRank.

## Performance

We run this on one of our behaviour graphs which consists of approximately 700M vertices and 15B edges. Using the following Spark configuration, and in-memory persistence of edge and vertex RDDs, we obtain iteration times of between 3 to 5 minutes each.

Configuration example:

 - Spark 2.1.1
 - YARN
 - Dynamic allocation: no
 - Number of executors: 256
 - Number of executor cores: 4
 - Executor memory: 28G

### Performance Tuning

- Persist the edges and vertices of the graph in memory and disk (as spill): `StorageLevel.MEMORY_AND_DISK`
- Enable Kryo serialization: `KryoSerialization.useKryo`

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

## Versioning

This library aims to adhere to [Semantic Versioning 2.0.0](http://semver.org/spec/v2.0.0.html). Violations of this scheme should be reported as bugs. Specifically, if a minor or patch version is released that breaks backward compatibility, that version should be immediately removed and/or a new version should be immediately released that restores compatibility. Breaking changes to the public API will only be introduced with new major versions.

## Contributing

We welcome contributions by [pull requests](https://github.com/soundcloud/spark-pagerank/pulls) and bug reports or feature requests as [issues](https://github.com/soundcloud/spark-pagerank/issues).

## Contact

Please contact [Josh Devins](mailto:josh@soundcloud.com) for more details, or with any questions or comments.

## License

Copyright (c) 2017 [SoundCloud Ltd.](http://soundcloud.com)

See the [LICENSE](LICENSE) file for details.
