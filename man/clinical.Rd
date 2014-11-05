\name{clinical}
\alias{clinical}
\alias{samples}
\alias{dna}
\docType{data}
\title{
Example sample tracking dataset
}
\description{
A synthetic sample tracking dataset consisting of a clinical table containing clinical information on a set given fictional patients (samples in this context).
A samples table which contains addition information on the samples in the clinical dataset including whether they have had DNA collected from them.  Finally there
is a DNA table which indicates the quality of a given DNA specimen as well as the ID provided by the DNA isolation lab.  See the vignette for more descriptions and
an example of loading and querying the data using \code{poplite}.
}
\usage{data("clinical")}
\format{
  The format is:
\tabular{ll}{
 "clinical" \tab data.frame \cr
 "samples" \tab data.frame \cr
 "dna" \tab data.frame \cr
}
}

\examples{
data(clinical)
str(clinical)
str(samples)
str(dna)
}
\keyword{datasets}
