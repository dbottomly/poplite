\name{Database-class}
\Rdversion{1.1}
\docType{class}
\alias{Database}
\alias{Database-class}
\alias{columns,Database-method}
\alias{dbFile}
\alias{dbFile,Database-method}
\alias{populate}
\alias{populate,Database-method}
\alias{schema}
\alias{schema,Database-method}
\alias{tables,Database-method}

\title{Class \code{"Database"}}
\description{
An object representing an SQLite database containing both the location of the file
as well as a \code{TableSchemaList} object describing the structure of the database.
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{Database(tbsl, db.file)}.
 
}
\section{Slots}{
  \describe{
    \item{\code{tbsl}:}{Object of class \code{"TableSchemaList"} representing the current or desired database schema}
    \item{\code{db.file}:}{Single file path to the desired location of the database}
  }
}
\section{Methods}{
  \describe{
    \item{columns}{\code{signature(obj = "Database")}: Returns a list of the database table columns indexed by table name}
    \item{dbFile}{\code{signature(obj = "Database")}: Returns the file path associated with the database}
    \item{populate}{\code{signature(obj = "Database")}, ins.vals, use.tables = NULL, should.debug = FALSE: Populate an SQLite database using the schema and location from the \code{Database} object and the data in the \code{ins.vals} list to be inserted as specified in the  \code{dta.func} element of the \code{TableSchemaList}.  The \code{use.tables} argument can be used to limit the tables populated.  The \code{should.debug} argument outputs more verbose messages regarding the SQL queries.}
    \item{schema}{\code{signature(obj = "Database")}: Returns the associated \code{TableSchemaList} object}
    \item{tables}{\code{signature(obj = "Database")}: Returns a character vector containing the table names}
	 }
}
\author{
Daniel Bottomly
}
\seealso{
	\code{\linkS4class{TableSchemaList}}
}
\examples{
showClass("Database")
}
\keyword{classes}
