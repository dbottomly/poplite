\name{Database-class}
\Rdversion{1.1}
\docType{class}
\alias{Database-class}
\alias{columns,Database-method}
\alias{dbFile,Database-method}
\alias{populate,Database-method}
\alias{schema,Database-method}
\alias{tables,Database-method}

\title{Class \code{"Database"}}
\description{
%%  ~~ A concise (1-5 lines) description of what the class is. ~~
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("Database", ...)}.
%%  ~~ describe objects here ~~ 
}
\section{Slots}{
  \describe{
    \item{\code{tbsl}:}{Object of class \code{"TableSchemaList"} ~~ }
    \item{\code{db.file}:}{Object of class \code{"character"} ~~ }
  }
}
\section{Methods}{
  \describe{
    \item{columns}{\code{signature(obj = "Database")}: ... }
    \item{dbFile}{\code{signature(obj = "Database")}: ... }
    \item{populate}{\code{signature(obj = "Database")}: ... }
    \item{schema}{\code{signature(obj = "Database")}: ... }
    \item{tables}{\code{signature(obj = "Database")}: ... }
	 }
}
\references{
%%  ~~put references to the literature/web site here~~
}
\author{
%%  ~~who you are~~
}
\note{
%%  ~~further notes~~
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
%%  ~~objects to See Also as \code{\link{~~fun~~}}, ~~~
%%  ~~or \code{\linkS4class{CLASSNAME}} for links to other classes ~~~
}
\examples{
showClass("Database")
}
\keyword{classes}
