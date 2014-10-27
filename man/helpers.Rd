\name{Helper Functions}
\alias{makeSchemaFromData}
\alias{makeSchemaFromFunction}
\alias{correct.df.names}

\title{
Functions to facilitate the creation of \code{poplite}\'s data structures.
}
\description{
These functions facilitate the creation of \code{TableSchemaList} objects from 
existing or supplementary R data structures such as the \code{data.frame}.
}
\usage{
makeSchemaFromData(tab.df, name=NULL, primary.cols=NULL,dta.func=NULL)
makeSchemaFromFunction(dta.func, name,\dots)
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
\item{dta.func}{
	An optional function which will take a specified object and turn it into a SQLite table.  The function parameters should have the same names as the objects supplied to \code{populate}.
}

\item{\dots}{
	Arbitary objects provided to the function specified in \code{dta.func}.  They should be of the same type and be named like the objects to be passed to \code{populate}.   
}
}
\value{

\item{makeSchemaFromData}{A \code{TableSchemaList} object}
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
    
    makeSchemaFromFunction(function(x) head(x),  name="team_franch", x=TeamsFranchises)

    test.df <- TeamsFranchises
    names(test.df)[1] <- "franch.ID"
    
    names(test.df)
    
    names(correct.df.names(test.df))
  }
  }

\keyword{ utilities}
