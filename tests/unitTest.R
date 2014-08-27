#borrowed from the Bioconductor folks

require(poplite, quietly=T)
require(RUnit, quietly=T)

suite <- defineTestSuite(name="poplite test suite", dirs=system.file("unitTests", package="poplite"),
	rngKind="default", rngNormalKind="default")

result <- runTestSuite(suite)
printTextProtocol(result, showDetails=T)

##borrowed from blme
tmp <- getErrors(result)

if(tmp$nFail > 0 | tmp$nErr > 0) {
    stop(paste("\n\nunit testing failed (#test failures: ", tmp$nFail,
               ", #R errors: ",  tmp$nErr, ")\n\n", sep=""))
  }