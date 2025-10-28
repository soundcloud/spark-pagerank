
# PageRank in Spark

This is an implementation of PageRank in Spark, using Spark's standard RDD API.

## Features

- Fast iterations
- Parameterised "teleport" probability
- Weighted edges (out-edge weights must be normalized)
- Supports "dangling" vertices (no out-edges from a node)
- Supports arbitrary (e.g. non-uniform) priors (as vertex values)
- Various stopping criteria:
  - Number of iterations threshold
  - Convergence threshold (requires additional computation after each iteration)
- Utilities for building, preparing, and validating input graphs (incl. out-edge normalization)

## Usage

Include it as a dependency in your sbt project:

### As A Library

You can use PageRank in Spark as a library and call it from within your own drivers. You will want to do this when you have some data preparation to do that does not conform with the built-in driver data interfaces.

More examples of usage as a library can be found in the source of the built-in drivers (see below).

### As Drivers

We include several built-in drivers that operate on plain-text TSV input of labeled edges as a starting point. You will prepare the graph and run PageRank in the following sequence of drivers. Use `--help` to see the arguments and usage of each driver.


## Performance

We run this library on one of our behavior graphs which consists of approximately 700M vertices and 15B edges. Using the following Spark configuration, and in-memory persistence of edge and vertex RDDs, we obtain iteration times on the order of 3-5 minutes each.

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

To publish an artifact to the Sonatype/Maven central repository (a snapshot or release), you need to have a Sonatype account, PGP keys and sbt plugins set up. Please follow the [sbt guideline](http://www.scala-sbt.org/release/docs/Using-Sonatype.html) for a complete guide to getting started. Once this is done, you can use the [sbt-release](https://github.com/sbt/sbt-release) plugin via the `Makefile` to publish snapshots and perform releases.

### Publishing Snapshots

At any point in the development lifecycle, a snapshot can be published to the central repository.

```
make publish
```

### Performing a Release

Once development of a version is complete, the artifact should be released to the central repository. This is a two stage process with the artifact first entering a staging repository, followed by a manual promotion process.

```
make release
```

After a release to the staging repository, the [staging-to-release promotion process](http://central.sonatype.org/pages/releasing-the-deployment.html) must be followed manually before the artifact is available in the central repository.

## Versioning

This library aims to adhere to [Semantic Versioning 2.0.0](http://semver.org/spec/v2.0.0.html). Violations of this scheme should be reported as bugs. Specifically, if a minor or patch version is released that breaks backward compatibility, that version should be immediately removed and/or a new version should be immediately released that restores compatibility. Breaking changes to the public API will only be introduced with new major versions.

## Contributing


## Authors

* [Josh Devins](https://github.com/joshdevins)

### Contributors

* [Max Jakob](https://github.com/maxjakob)

## License


See the [LICENSE](LICENSE) file for details.
