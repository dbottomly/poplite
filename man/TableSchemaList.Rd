\name{TableSchemaList-class}
\Rdversion{1.1}
\docType{class}
\alias{TableSchemaList-class}
\alias{TableSchemaList}
\alias{createTable,TableSchemaList-method}
\alias{createTable}
\alias{insertStatement,TableSchemaList-method}
\alias{insertStatement}
\alias{mergeStatement,TableSchemaList-method}
\alias{mergeStatement}
\alias{populate}
\alias{populate,TableSchemaList-method}
\alias{append,TableSchemaList,TableSchemaList-method}
\alias{columns,TableSchemaList-method}
\alias{length,TableSchemaList-method}
\alias{relationship<-,TableSchemaList-method}
\alias{tables,TableSchemaList-method}

\title{Class \code{"TableSchemaList"}}
\description{
A list-based representation of a SQLite database which provides a simple approach to loading data into a database as well as merging with the existing data.
}
\section{Objects from the Class}{
Objects can be created by calls of the form \code{new("TableSchemaList", tab.list, search.cols)}.
}
\section{Slots}{
  \describe{
    \item{\code{tab.list}:}{Object of class \code{"list"} A list of lists with each list representing a table and each element containing information on the
    	definition of columns.  There should be 6 elements to the list: 
    	\code{db.cols} a character vector containing the names of the columns
    	\code{db.schema} a character vector of the same length as \code{db.cols} which contains the columns types (e.g. TEXT, INTEGER)
    	\code{db.constr} a character string containing the statement at the end of a query indicating constraints
    	\code{dta.func} a function which when applied to the input (usually a list) provides a \code{data.frame} to be inserted into the database.
    	\code{should.ignore} a boolean value indicating whether duplicates implied by the constraints should be ignored upon insertion
    	\code{foreign.keys} a list (or NULL) containing several elements named by each table to be joined.  The two elements are \code{local.keys} which are
    	the columns that should be kept from joining of the two tables and \code{ext.keys} which are the columns used in the joining.}
  }
}
\section{Methods}{
  \describe{
  	\item{length}{\code{signature(obj = "TableSchemaList")} Return the number of tables in the object}
  	\item{append}{\code{signature(obj = "TableSchemaList")}, x, values, after=length(x): Return a new \code{TableSchemaList} object consisting of \code{x}, the object to be modified, \code{values} the object(s) to be added and \code{after} the element of \code{x} to place them after.}
  	\item{columns}{\code{signature(obj = "TableSchemaList")} Returns a list of length equal to the number of tables where each element contains columns for the given table.}
  	\item{tables}{\code{signature(obj = "TableSchemaList")} Returns a vector of the table names in the object.}
  	\item{populate}{\code{signature(obj = "TableSchemaList")}, db.con, ins.vals, use.tables = NULL, should.debug = FALSE: Populate an SQLite database provided in db.con, using the schema specified in TableSchemaList and the data in the ins.vals list to be inserted as specified in the  \code{dta.func} element of obj.  The \code{use.tables} argument can be used to limit the tables populated.  The \code{should.debug} argument outputs more verbose messages regarding the SQL queries.}
    \item{createTable}{\code{signature(obj = "TableSchemaList")}, table.name, mode=c("normal", "merge"): Produces a create table statement based on the table specified in \code{table.name} and whether the table should be temporary for merging purposes or normal permanent table}
    \item{insertStatement}{\code{signature(obj = "TableSchemaList")}, table.name, mode=c("normal", "merge"): Produces an insert statement based on the table specified in \code{table.name} and whether the table should be temporary for merging purposes or normal permanent table.  This insert statement will be used in conjunction with \code{dbGetPreparedQuery} in the RSQLite package and the \code{data.frame} resulting from the \code{dta.func} function to populate the initial database table.}
    \item{mergeStatement}{\code{signature(obj = "TableSchemaList")}, table.name: Produces a statement joining an existing table and a temporary one and inserting into a new (non-temporary) tables}
}}

\author{
Daniel Bottomly
}

\examples{
tbsl <- SangerVarMask()

createTable(tbsl, table.name="reference", mode="normal")
createTable(tbsl, table.name="reference", mode="merge")

insertStatement(tbsl, table.name="reference", mode="normal")
insertStatement(tbsl, table.name="reference", mode="merge")

mergeStatement(tbsl, table.name="reference")

}
\keyword{classes}
