require("RUnit", quietly = TRUE)

testResult <- runTestFile("tests_tbsl.R")
printTextProtocol(testResult, showDetails = TRUE)


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

#currently no tests for the presupplied masks, correct.db.names, the Database and TableSchemaList classes.

test.tbsl <- function()
{
    #makeSchemaFromData
    
    #append
    
    #length
    
    #relationship
}

test.Database.functions <- function()
{
    #columns
    
    #tables
    
    #dbFile
    
    #schema
    
    #filter
    
    
    #select
    
    
}

#will not work for now until applicable data is available...
#test.populate <- function()
#{
#    #populate a subset of the tbsl using the example data  
#    
#    .tc.func <- function(x)
#    {
#        temp.x <- x[!duplicated(x$Transcript.Cluster.ID),"Transcript.Cluster.ID", drop=FALSE]
#        names(temp.x) <- "TC_ID"
#        return(temp.x)
#    }
#    
#    .probe.func <- function(x)
#    {
#        temp.x <- x[,c("Probe.ID", "probe.x", "probe.y", "Transcript.Cluster.ID")]
#        names(temp.x) <- c("Probe_ID", "Probe_X", "Probe_Y", "TC_ID")
#        return(temp.x)
#    }
#    
#    tab.list <- list(transcript_cluster=list(db.cols=c("TC_PK", "TC_ID"),
#                                db.schema=c("INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER"),
#                                db.constr="",
#                                dta.func=.tc.func, should.ignore=FALSE, foreign.keys=NULL),
#        probe=list(db.cols=c("Probe_PK", "Probe_ID", "Probe_X", "Probe_Y", "TC_PK"),
#                   db.schema=c("INTEGER PRIMARY KEY AUTOINCREMENT", "INTEGER", "INTEGER", "INTEGER", "INTEGER"),
#                   db.constr="",
#                   dta.func=.probe.func, should.ignore=FALSE, foreign.keys=list(transcript_cluster=list(ext.keys="TC_ID", local.keys="TC_PK"))))
#    
#    tbsl <- new("TableSchemaList", tab.list=tab.list)
#    
#    db.con <- dbConnect(SQLite(), tempfile())
#    
#    probe.tab.file <- om.tab.file()
#    
#    probe.tab <- read.delim(probe.tab.file, sep="\t", header=TRUE, stringsAsFactors=FALSE)
#    
#    populate.db.tbl.schema.list(db.con, db.schema=tbsl, ins.vals=probe.tab, use.tables=NULL, should.debug=TRUE)
#
#    test.query <- dbGetQuery(db.con, "SELECT Probe_ID, Probe_X, Probe_Y, TC_ID FROM probe JOIN transcript_cluster USING (TC_PK)")
#    names(test.query) <- c("Probe.ID", "probe.x", "probe.y", "Transcript.Cluster.ID")
#    
#    test.query <- test.query[do.call("order", test.query),]
#    rownames(test.query) <- NULL
#    
#    sub.probe.tab <- probe.tab[,c("Probe.ID", "probe.x", "probe.y", "Transcript.Cluster.ID")]
#    sub.probe.tab <- sub.probe.tab[do.call("order", sub.probe.tab),]
#    rownames(sub.probe.tab) <- NULL
#    
#    checkEquals(test.query, sub.probe.tab)
#    
#    dbDisconnect(db.con)
#    
#    #should be able to easily populate a single table without dependencies
#    
#    db.con <- dbConnect(SQLite(), tempfile())
#    
#    populate.db.tbl.schema.list(db.con, db.schema=tbsl, ins.vals=probe.tab, use.tables="transcript_cluster", should.debug=TRUE)
#    
#    comp.tab <- .tc.func(probe.tab)
#    comp.tab <- cbind(TC_PK=1:nrow(comp.tab), comp.tab)
#    rownames(comp.tab) <- NULL
#    
#    test.query.2 <- dbGetQuery(db.con, "SELECT * FROM transcript_cluster")
#    
#    checkEquals(comp.tab, test.query.2)
#    
#    dbDisconnect(db.con)
#    
#    #should throw an error if attempting to populate a table with dependencies
#    
#    db.con <- dbConnect(SQLite(), tempfile())
#    checkException(populate.db.tbl.schema.list(db.con, db.schema=tbsl, ins.vals=probe.tab, use.tables="probe", should.debug=TRUE))
#    
#    dbDisconnect(db.con)
#}



test.createTable <- function()
{
    tbsl <- new("TableSchemaList")
    
    valid.tables <- names(tbsl@tab.list)
    
    db.con <- dbConnect(SQLite(), tempfile())
    
    for(i in valid.tables)
    {
        print(i)
        for (j in c("normal", "merge"))
        {
            print(j)
            
            f.keys <- tbsl@tab.list[[i]]$foreign.keys
            
            #if there are no foreign keys available, don't allow create table statements to be generated
            if (j == "merge" && is.null(f.keys))
            {
                checkException(createTable(tbsl, table.name=i, mode="merge"))
            }
            else
            {
                if (is.null(f.keys) || j == "normal")
                {
                    add.cols <- character(0)
                    add.type <- character(0)
                    add.pk <- integer(0)
                }
                else
                {
                    #the basic table should already exist so can retrieve the previous info on coltypes
                    
                    temp.prag <- do.call("rbind", lapply(names(f.keys), function(x)
                           {
                                dbGetQuery(db.con, paste("pragma table_info(",x,")"))
                           }))
                    
                    key.vals <- as.character(unlist(sapply(f.keys, "[[", "ext.keys")))
                    key.prag <- temp.prag[temp.prag$name %in% key.vals,]
                    add.cols <- key.prag$name
                    add.type <- key.prag$type
                    add.pk <- rep(0L, length(add.type))
                }
                
                prag.tab.name <- ifelse(j=="merge", paste0(i, "_temp"), i)
                
                checkTrue(is.null(dbGetQuery(db.con, createTable(tbsl, table.name=i, mode=j))))
                tab.prag <- dbGetQuery(db.con, paste("pragma table_info(",prag.tab.name,")"))
                sub.prag <- tab.prag[,c("name", "type", "pk")]
                
                col.types <- sapply(strsplit(tbsl@tab.list[[i]]$db.schema, "\\s+"), "[", 1)
                col.names <- tbsl@tab.list[[i]]$db.cols
                is.pk <- as.integer(grepl("PRIMARY KEY", tbsl@tab.list[[i]]$db.schema))
                
                col.names <- append(col.names, add.cols)
                col.types <- append(col.types, add.type)
                is.pk <- append(is.pk, add.pk)
                
                query.dta <- data.frame(name=col.names, type=col.types, pk=is.pk, stringsAsFactors=FALSE)
                
                if (j == "merge")
                {
                    query.dta <- query.dta[query.dta$pk == 0 & query.dta$name %in% sapply(f.keys, "[[", "local.keys") == FALSE,]
                }
                
                ord.prag <- sub.prag[do.call("order", sub.prag),]
                ord.query <- query.dta[do.call("order", query.dta),]
                
                rownames(ord.prag) <- NULL
                rownames(ord.query) <- NULL
                
                checkEquals(ord.prag, ord.query)
            }
        }
        
    }
    
    dbDisconnect(db.con)
}

test.insertStatement <- function()
{
    set.seed(123)
    
    tbsl <- new("TableSchemaList")
    
    valid.tables <- names(tbsl@tab.list)
    
    db.con <- dbConnect(SQLite(), tempfile())
    
    for(i in valid.tables)
    {
        print(i)
        for(j in c("normal", "merge"))
        {
            print(j)
            f.keys <- tbsl@tab.list[[i]]$foreign.keys
            
            if (j == "merge" && is.null(f.keys))
            {
                checkException(insertStatement(tbsl, i, j))
            }
            else
            {
                #first create the tables
                checkTrue(is.null(dbGetQuery(db.con, createTable(tbsl, table.name=i, mode=j))))
                
                prag.tab.name <- ifelse(j=="merge", paste0(i, "_temp"), i)
                tab.prag <- dbGetQuery(db.con, paste("pragma table_info(",prag.tab.name,")"))
                
                #create a couple lines of fake data to insert into the database
                
                ins.dta <- as.data.frame(matrix(sample.int(10000, 10*nrow(tab.prag)), ncol=nrow(tab.prag), nrow=10, dimnames=list(NULL, tab.prag$name)), stringsAsFactors=fALSE)
                
                for(p in colnames(ins.dta))
                {
                    if (tab.prag$type[tab.prag$name == p] == "TEXT")
                    {
                        ins.dta[,p]  <- as.character(ins.dta[,p])
                    }
                }
                
                #load into the database
                
                dbBeginTransaction(db.con)
                checkTrue(is.null(dbGetPreparedQuery(db.con, insertStatement(tbsl, i, mode=j), bind.data = ins.dta)))
                dbCommit(db.con)
                
                #check whether it respects should.ignore
                
                ignore.match <- regexpr(pattern="INSERT\\s+OR\\s+IGNORE", text=insertStatement(tbsl, i, mode=j), perl=TRUE)
                
                if (tbsl@tab.list[[i]]$should.ignore)
                {
                    checkTrue(ignore.match != -1)
                }
                else
                {
                    checkTrue(ignore.match == -1)
                }
            }
        }
    }
    
    dbDisconnect(db.con)
}

test.mergeStatement <- function()
{  
    tbsl <- new("TableSchemaList")
    
    valid.tables <- names(tbsl@tab.list)
    
    for(i in valid.tables)
    {
        #again if there are no foreign keys make sure the query dies
        f.keys <- tbsl@tab.list[[i]]$foreign.keys
        print(i)
        if (is.null(f.keys))
        {
            checkException(mergeStatement(tbsl, i))
        }
        else
        {
            cur.stat <- mergeStatement(tbsl, i)
            
            #is the table definition consistent
            
            tab.match <- regexpr(pattern=paste0(i, "\\s+\\(\\s+([\\w+_,]+)\\s+\\)"), text=cur.stat, perl=TRUE)
            tab.str <- substr(cur.stat, start=attr(tab.match, "capture.start"), stop=attr(tab.match, "capture.start")+attr(tab.match, "capture.length")-1)
            split.tab <- strsplit(tab.str, ",")[[1]]
            
            tab.cols <- tbsl@tab.list[[i]]$db.cols
            tab.cols <- tab.cols[tbsl@tab.list[[i]]$db.schema != "INTEGER PRIMARY KEY AUTOINCREMENT"]
            
            checkTrue(length(intersect(split.tab, tab.cols)) == length(union(split.tab, tab.cols)))
            
            #is the select statement consistent with the table definition
            
            select.match <- regexpr(pattern=paste0("SELECT\\s+", tab.str), text=cur.stat, perl=TRUE)
            
            checkTrue(select.match != -1)
            
            #are the joins sane
            
            join.base <- sapply(f.keys, function(x) paste0("\\(", paste(x$ext.keys, collapse=","), "\\)"))
            
            join.str <- paste0(i, "_temp\\s+", paste(paste("JOIN", names(join.base), "USING", join.base, sep="\\s+"), collapse="\\s+"))
            
            join.match <- regexpr(pattern=join.str, text=cur.stat, perl=TRUE)
            
            checkTrue(join.match != -1)
            
            #is it respecting should.ignore
            
            ignore.match <- regexpr(pattern="INSERT\\s+OR\\s+IGNORE", text=cur.stat, perl=TRUE)
            
            if (tbsl@tab.list[[i]]$should.ignore)
            {
                checkTrue(ignore.match != -1)
            }
            else
            {
                checkTrue(ignore.match == -1)
            }
        }
    }
}

