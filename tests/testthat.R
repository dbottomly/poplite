library(testthat)
library(Lahman)

test.db.1 <- function()
{
    samples <- data.frame(sample_id=as.integer(sapply(1:100, function(x) rep(x,2))), wave=as.integer(sapply(1:100, function(x) 1:2)), did_collect=sample(c("Y", "N"), size=100, replace=T))
    
    dna <- samples[samples$did_collect == "Y",c("sample_id", "wave")]
    dna$lab_id <- paste("dna", paste(dna$sample_id, dna$wave, sep="_"), sep="_")
    dna$lab_id[sample.int(nrow(dna), size=10)] <- NA
    dna$ng.ul <- abs(rnorm(n=nrow(dna), mean=146, sd=98))
    dna$ng.ul[is.na(dna$lab_id)] <- NA
        
    clinical <- data.frame(sample_id=1:100, sex=sample(c("M", "F"), size=100, replace=T), age=sample(1:20, 100, replace=T), status=sample(c(0L,1L), size=100, replace=T), var_wave_1=rnorm(n=100), var_wave_2=rnorm(n=100))
    clinical <- clinical[-sample.int(nrow(clinical), 12),]
    
    return(list(samples=samples, dna=dna, clinical=clinical))
}


test_check("poplite")
