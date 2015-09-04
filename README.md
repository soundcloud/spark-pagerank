# spark-pagerank

PageRank in Spark with GraphX.

Please contact the [Josh](mailto:josh@soundcloud.com) for questions or comments.

## Usage

Include it as a dependency in your sbt project:
`"com.soundcloud" %% "spark-pagerank" % <version>`

## Features

- convergence threshold
- number of iterations threshold
- parameterized "teleport" probability
- weighted edges (as normalized values in edge attributes)
- supports "dangling" node (no out edges)
- supports non-uniform priors (as normalized values in vertex attributes)

## Contributors

- [Max Jakob](mailto:max@soundcloud.com)
  - migrated the Pregel-form Spark PageRank implementation and did some cleanup
  - added support for "dangling" nodes
