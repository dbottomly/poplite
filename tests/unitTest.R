#borrowed from the Bioconductor folks

require(poplite, quietly=T)
require(RUnit, quietly=T)

suite <- defineTestSuite(name="poplite test suite", dirs=system.time("unitTests", package="poplite"),
	rngKind="default", rngNormalKind="default")

result <- runTestSuite(suite)
printTextProtocol(result)
