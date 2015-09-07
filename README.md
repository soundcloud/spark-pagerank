# spark-pagerank

PageRank in Spark with GraphX.

Please contact [Josh](mailto:josh@soundcloud.com) for questions or comments.

## Usage

Include it as a dependency in your sbt project:
`"com.soundcloud" %% "spark-pagerank" % <version>`

## Features

- convergence threshold
- number of iterations threshold
- parameterized "teleport" probability
- weighted edges (as normalized values in edges)
- supports "dangling" vertices (no out edges)
- supports non-uniform priors (as values in vertices)
