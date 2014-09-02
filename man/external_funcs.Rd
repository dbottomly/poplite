\name{External methods}
\alias{filter}
\alias{select}
\alias{filter.Database}
\alias{select.Database}

\title{
Specific methods for generics defined in external packages.
}
\description{
These functions provide convienient interfaces to functionality provided in external packages (currently only \pkg{dplyr}).
}
\usage{
\method{filter}{Database}(.data, \dots)
\method{select}{Database}(.data,\dots, .tables=NULL)
}

\arguments{
  \item{.data}{
A \code{Database} object.
}
  \item{.tables}{
  	A character vector indicating the table(s) the specified columns refer to.
  }
  \item{\dots}{
For \code{filter.Database}, a single valid R expression which would result in a logical vector upon execution.  
For \code{select.Database}, expression indicating the columns to choose from the given table(s).  See the examples
in \code{dplyr::filter} and \code{dplyr::select}.  In addition, the names of the tables can be prepended to each 
variable name similar to SQL statements (e.g. 'table.column').
}
}

\value{
An object of class \code{tbl_sqlite}.
}

\author{
Daniel Bottomly
}
\seealso{
\code{\link[dplyr]{filter}},\code{\link[dplyr]{select}}
}
\examples{
	print ('hello')
}

\keyword{utilities}
