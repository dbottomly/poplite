\name{External methods}
\alias{filter.Database}
\alias{select.Database}

\title{
Specific methods for generics defined in external packages.
}
\description{

}
\usage{
filter.Database(.data, ...)
select.Database(.data,..., .tables=NULL)
}
%- maybe also 'usage' for other objects documented here.
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
%%  ~Describe the value returned
%%  If it is a LIST, use
%%  \item{comp1 }{Description of 'comp1'}
%%  \item{comp2 }{Description of 'comp2'}
%% ...
}
\references{
%% ~put references to the literature/web site here ~
}
\author{
Daniel Bottomly
}
\seealso{
%% ~~objects to See Also as \code{\link{help}}, ~~~
}
\examples{
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{utilities}
\keyword{ ~kwd2 }% __ONLY ONE__ keyword per line
