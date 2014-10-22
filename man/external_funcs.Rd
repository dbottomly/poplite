\name{External methods}
\alias{filter}
\alias{select}

\title{
Specific methods for generics defined in external packages.
}
\description{
These functions provide convienient interfaces to functionality provided in external packages (currently only \pkg{dplyr}).
See the vignette and below examples.
}
\usage{
filter(.data, \dots)
select(.data, \dots,.tables=NULL)

}

\arguments{
  \item{.data}{
A \code{Database} object.
}
  \item{.tables}{
  	A character vector indicating the table(s) the specified columns refer to.
  }
  \item{\dots}{
For \code{filter}, a single valid R expression which would result in a logical vector upon execution.  
For \code{select}, expression indicating the columns to choose from the given table(s).  See the examples
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
  if (require(Lahman))
  {
      baseball.teams <- makeSchemaFromData(TeamsFranchises, name="team_franch")
      baseball.teams <- append(baseball.teams, makeSchemaFromData(Teams, name="teams"))
      
      relationship(baseball.teams, from="team_franch", to="teams") <- franchID ~ franchID
      
      baseball.db <- Database(baseball.teams, tempfile())
      
      populate(baseball.db, list(teams=Teams, team_franch=TeamsFranchises))
      
      select(baseball.db, .tables="teams")
      
      select(baseball.db, .tables=c("teams", "team_franch"))
      
      select(baseball.db, yearID:WCWin, franchName)
      
      filter(baseball.db, active == "Y")
      
      select(filter(baseball.db, active == "Y" & W > 50 & teamID == "CAL"), active, W, teamID)
  }
}

\keyword{utilities}
