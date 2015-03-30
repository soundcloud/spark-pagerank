# spark-pagerank

PageRank in Spark with GraphX.

Please contact the [Discovery Team](mailto:discovery@soundcloud.com) for
questions or comments.

## Usage

Include it as a dependency in your sbt project:
`"com.soundcloud" %% "spark-pagerank" % <version>`

## Features

- convergence threshold
- number of iterations threshold
- parameterized "teleport" probability
- weighted edges
- supports "dangling" node (no out edges)
- optional initialization with starting PageRank vector
- optional non-uniform priors (TBD)
