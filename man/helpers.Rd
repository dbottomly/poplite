\name{Helper Functions}
\alias{makeSchemaFromData}
\alias{correct.df.names}

\title{
Functions to facilitate the creation of \code{poplite}\'s data structures.
}
\description{
These functions facilitate the creation of \code{TableSchemaList} objects from 
existing or supplementary R data structures such as the \code{data.frame}.
}
\usage{
makeSchemaFromData(tab.df, name=NULL, primary.cols=NULL)
correct.df.names(tab.df)
}

\arguments{
  \item{tab.df}{
A \code{data.frame} representing a database table
}
  \item{name}{
Desired name of the database table
}
\item{primary.cols}{
	An optional character vector containing the columns that uniquely identify a given row.  If missing or NULL,
	an autoincremented integer is used instead.
}
}

\value{

\item{makeSchemaFromData}{A \code{TableSchemaList} object}
\item{correct.df.names}{A \code{data.frame} with valid names for SQLite}

}

\author{
Daniel Bottomly
}

\seealso{
	\code{\linkS4class{TableSchemaList}}
}
\examples{
  if (require(Lahman))
  { 
    franches <- makeSchemaFromData(TeamsFranchises, name="team_franch")
    show(franches)
    
    test.df <- TeamsFranchises
    names(test.df)[1] <- "franch.ID"
    
    names(test.df)
    
    names(correct.df.names(test.df))
  }
  }

\keyword{ utilities}
