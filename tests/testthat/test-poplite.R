#currently no tests for the presupplied masks, correct.db.names, the Database and TableSchemaList classes.

check.table.import <- function(dta, tbsl, name, pks=paste(name, "ind", sep="_"))
{
    expect_is(tbsl, "TableSchemaList")
    expect_named(tbsl@tab.list, name)
    #are the column names ok
    expect_equal(length(intersect(tbsl@tab.list[[name]]$db.cols, union(names(dta), pks))), length(union(tbsl@tab.list[[name]]$db.cols, union(names(dta), pks))))
    
    #are the column types ok in a basic sense
    ##just removes autoincremented PKs
    common.cols <- intersect(tbsl@tab.list[[name]]$db.cols, names(dta))
    
    tab.cols <- sapply(names(dta), function(x) class(dta[,x]))
    tab.cols <- toupper(tab.cols)
    tab.cols[tab.cols %in% c("CHARACTER", "FACTOR")] <- "TEXT"
    
    expect_identical(tab.cols, tbsl@tab.list[[name]]$db.schema[names(tab.cols)])
}

#these are simple relationships so the keys will be equivalent and there should be no modifications of columns etc
check.direct.keys <- function(tbsl, from, to, key.name, orig.to.obj, orig.from.obj)
{
    expect_named(tbsl@tab.list[[to]]$foreign.keys, from)
    expect_identical(tbsl@tab.list[[to]]$foreign.keys[[from]]$local.keys, tbsl@tab.list[[to]]$foreign.keys[[from]]$ext.keys)
    
    expect_identical(tbsl@tab.list[[to]]$foreign.keys[[from]]$local.keys, key.name)
    
    expect_identical(tbsl@tab.list[[to]]$db.col, orig.to.obj@tab.list[[to]]$db.col)
    expect_identical(tbsl@tab.list[[to]]$db.schema, orig.to.obj@tab.list[[to]]$db.schema)
    
    expect_identical(tbsl@tab.list[[from]]$db.col, orig.from.obj@tab.list[[from]]$db.col)
    expect_identical(tbsl@tab.list[[from]]$db.schema, orig.from.obj@tab.list[[from]]$db.schema)
}


test_that("Create and work with TBSL and Database objects in a basic sense",
{
    #makeSchemaFromData, append and length
    
    baseball.teams <- new("TableSchemaList")
    
    expect_equal(length(baseball.teams), 0)
    
    franches <- makeSchemaFromData(TeamsFranchises, name="team_franch")
    check.table.import(TeamsFranchises, franches, "team_franch")
    
    baseball.teams <- append(baseball.teams, franches)
    
    expect_equal(length(baseball.teams), 1)
    
    teams <- makeSchemaFromData(Teams, name="teams")
    check.table.import(Teams, teams, "teams")
    
    baseball.teams <- append(baseball.teams, teams)
    
    expect_equal(length(baseball.teams), 2)
    
    salaries <- makeSchemaFromData(Salaries, name="salaries")
    check.table.import(Salaries, salaries, "salaries")
    
    baseball.teams <- append(baseball.teams, salaries)
    
    expect_equal(length(baseball.teams), 3)
    
    #relationships
    
    relationship(baseball.teams, from="team_franch", to="teams") <- franchID ~ franchID
    check.direct.keys(baseball.teams,  from="team_franch", to="teams", key.name="franchID", orig.to.obj=teams, orig.from.obj=franches)
    
    relationship(baseball.teams, from="teams", to="salaries") <- teamID ~ teamID
    check.direct.keys(baseball.teams,  from="teams", to="salaries", key.name="teamID", orig.to.obj=salaries, orig.from.obj=teams)
    
    #helpers for TableSchemaLists
    
    col.list <- columns(baseball.teams)
    
    expect_named(col.list, c("team_franch", "teams", "salaries"))
    expect_equal(col.list, list(team_franch=c("team_franch_ind", names(TeamsFranchises)), teams=c("teams_ind", names(Teams)), salaries=c("salaries_ind", names(Salaries))))
    
    expect_equal(tables(baseball.teams), c("team_franch", "teams", "salaries"))
    
    #Basic formation and checks of Database objects
    
    baseball.db <- Database(baseball.teams, "test_baseball_db.db")
    
    #columns
    
    expect_equal(columns(baseball.db), columns(baseball.teams))
    
    #tables
    
    expect_equal(tables(baseball.db), tables(baseball.teams))
    
    #dbFile
    
    expect_equal(dbFile(baseball.db), "test_baseball_db.db")
    
    #schema
    
    expect_equal(schema(baseball.db)@tab.list, baseball.teams@tab.list)
    
    #maybe not the best way to do this, though haven't seen another aside from re-creating the object
    assign(x="baseball.db", value=baseball.db, envir=.GlobalEnv)
})

test_that("Another, more complex TBSL example based off a sample tracking use case",{
    
    db.list <- test.db.1()
    
    sample.tracking <- new("TableSchemaList")
    
    clinical <- makeSchemaFromData(db.list$clinical, name="clinical")
    check.table.import(db.list$clinical, clinical, "clinical")
    
    sample.tracking <- append(sample.tracking, clinical)
    
    expect_equal(length(sample.tracking), 1)
    
    #this one should fail due to ng.ul column
    expect_error(makeSchemaFromData(db.list$dna, name="dna"))
    
    db.list$dna <- correct.df.names(db.list$dna)
    
    dna <- makeSchemaFromData(db.list$dna, name="dna")
    check.table.import(db.list$dna, dna, "dna")
    
    sample.tracking <- append(sample.tracking, dna)
    
    samples <- makeSchemaFromData(db.list$samples, name="samples")
    check.table.import(db.list$samples, samples, "samples")
    
    sample.tracking <- append(sample.tracking, samples)
    
    expect_equal(length(sample.tracking), 3)
    
    #more complicated usage of relationship
    
    relationship(sample.tracking, from="clinical", to="samples") <- sample_id~sample_id
    check.direct.keys(sample.tracking,  from="clinical", to="samples", key.name="sample_id", orig.to.obj=samples, orig.from.obj=clinical)
    
    relationship(sample.tracking, from="clinical", to="dna") <-sample_id~sample_id
    check.direct.keys(sample.tracking,  from="clinical", to="dna", key.name="sample_id", orig.to.obj=dna, orig.from.obj=clinical)
    
    #Here, db.cols (and db.schema) should be modified so that sample and wave in samples should be replaced with dna's autoinc pk
    relationship(sample.tracking, from="dna", to="samples") <- .~sample_id+wave
    #if there was no clinical to samples rels would expect below however will need to keep sample_id to be consistent with other relationships
    #expect_equal(sort(sample.tracking@tab.list$samples$db.cols), sort(c("samples_ind", "dna_ind", names(db.list$samples)[names(db.list$samples) %in% c("sample_id", "wave")==F])))
    
    expect_equal(sort(sample.tracking@tab.list$samples$db.cols), sort(c("samples_ind", "dna_ind", names(db.list$samples)[names(db.list$samples) %in% "wave"==F])))
    
    #this should not be true for dna
    expect_equal(sort(sample.tracking@tab.list$dna$db.cols), sort(c("dna_ind", names(db.list$dna))))
    
    #also check that the keys look sane
    expect_named(sample.tracking@tab.list$samples$foreign.keys, c("clinical", "dna"))
    
    #should just be the direct keys for clinical
    expect_equal(sample.tracking@tab.list$samples$foreign.keys$clinical, list(local.keys="sample_id", ext.keys="sample_id"))
    
    #should be sample_id and wave as well as dna's pk
    
    expect_equal(sample.tracking@tab.list$samples$foreign.keys$dna, list(local.keys="dna_ind", ext.keys=c("sample_id", "wave")))
    
    assign(x="sample.tracking",  value=sample.tracking, envir=.GlobalEnv)
})

test_that("createTable",
{
    tbsl <- sample.tracking
    
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
            if (j == "merge" && poplite:::shouldMerge(tbsl, i)==F)
            {
                expect_error(createTable(tbsl, table.name=i, mode="merge"))
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
                    #the basic table should already exist so can retrieve the previous info on coltyps
                    
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
                
                expect_true(is.null(dbGetQuery(db.con, createTable(tbsl, table.name=i, mode=j))))
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
                    #need to add in constrains similar to shouldMerge here...
                    query.dta <- query.dta[query.dta$pk == 0 & query.dta$name %in% sapply(f.keys, "[[", "local.keys") == FALSE,]
                }
                
                ord.prag <- sub.prag[do.call("order", sub.prag),]
                ord.query <- query.dta[do.call("order", query.dta),]
                
                rownames(ord.prag) <- NULL
                rownames(ord.query) <- NULL
                
                expect_equal(ord.prag, ord.query)
            }
        }
        
    }
    
    dbDisconnect(db.con)
})

test_that("Database population",{
    
    #simple example first
    columns(baseball.db)
})

test_that("Querying with Database objects",
{
    columns(baseball.db)
    #onto querying Database objects, probably do this after testing populate in a different function
    
    #filter
    
    #select
    
    
})

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




#
#test.insertStatement <- function()
#{
#    set.seed(123)
#    
#    tbsl <- new("TableSchemaList")
#    
#    valid.tables <- names(tbsl@tab.list)
#    
#    db.con <- dbConnect(SQLite(), tempfile())
#    
#    for(i in valid.tables)
#    {
#        print(i)
#        for(j in c("normal", "merge"))
#        {
#            print(j)
#            f.keys <- tbsl@tab.list[[i]]$foreign.keys
#            
#            if (j == "merge" && is.null(f.keys))
#            {
#                checkException(insertStatement(tbsl, i, j))
#            }
#            else
#            {
#                #first create the tables
#                checkTrue(is.null(dbGetQuery(db.con, createTable(tbsl, table.name=i, mode=j))))
#                
#                prag.tab.name <- ifelse(j=="merge", paste0(i, "_temp"), i)
#                tab.prag <- dbGetQuery(db.con, paste("pragma table_info(",prag.tab.name,")"))
#                
#                #create a couple lines of fake data to insert into the database
#                
#                ins.dta <- as.data.frame(matrix(sample.int(10000, 10*nrow(tab.prag)), ncol=nrow(tab.prag), nrow=10, dimnames=list(NULL, tab.prag$name)), stringsAsFactors=fALSE)
#                
#                for(p in colnames(ins.dta))
#                {
#                    if (tab.prag$type[tab.prag$name == p] == "TEXT")
#                    {
#                        ins.dta[,p]  <- as.character(ins.dta[,p])
#                    }
#                }
#                
#                #load into the database
#                
#                dbBeginTransaction(db.con)
#                checkTrue(is.null(dbGetPreparedQuery(db.con, insertStatement(tbsl, i, mode=j), bind.data = ins.dta)))
#                dbCommit(db.con)
#                
#                #check whether it respects should.ignore
#                
#                ignore.match <- regexpr(pattern="INSERT\\s+OR\\s+IGNORE", text=insertStatement(tbsl, i, mode=j), perl=TRUE)
#                
#                if (tbsl@tab.list[[i]]$should.ignore)
#                {
#                    checkTrue(ignore.match != -1)
#                }
#                else
#                {
#                    checkTrue(ignore.match == -1)
#                }
#            }
#        }
#    }
#    
#    dbDisconnect(db.con)
#}
#
#test.mergeStatement <- function()
#{  
#    tbsl <- new("TableSchemaList")
#    
#    valid.tables <- names(tbsl@tab.list)
#    
#    for(i in valid.tables)
#    {
#        #again if there are no foreign keys make sure the query dies
#        f.keys <- tbsl@tab.list[[i]]$foreign.keys
#        print(i)
#        if (is.null(f.keys))
#        {
#            checkException(mergeStatement(tbsl, i))
#        }
#        else
#        {
#            cur.stat <- mergeStatement(tbsl, i)
#            
#            #is the table definition consistent
#            
#            tab.match <- regexpr(pattern=paste0(i, "\\s+\\(\\s+([\\w+_,]+)\\s+\\)"), text=cur.stat, perl=TRUE)
#            tab.str <- substr(cur.stat, start=attr(tab.match, "capture.start"), stop=attr(tab.match, "capture.start")+attr(tab.match, "capture.length")-1)
#            split.tab <- strsplit(tab.str, ",")[[1]]
#            
#            tab.cols <- tbsl@tab.list[[i]]$db.cols
#            tab.cols <- tab.cols[tbsl@tab.list[[i]]$db.schema != "INTEGER PRIMARY KEY AUTOINCREMENT"]
#            
#            checkTrue(length(intersect(split.tab, tab.cols)) == length(union(split.tab, tab.cols)))
#            
#            #is the select statement consistent with the table definition
#            
#            select.match <- regexpr(pattern=paste0("SELECT\\s+", tab.str), text=cur.stat, perl=TRUE)
#            
#            checkTrue(select.match != -1)
#            
#            #are the joins sane
#            
#            join.base <- sapply(f.keys, function(x) paste0("\\(", paste(x$ext.keys, collapse=","), "\\)"))
#            
#            join.str <- paste0(i, "_temp\\s+", paste(paste("JOIN", names(join.base), "USING", join.base, sep="\\s+"), collapse="\\s+"))
#            
#            join.match <- regexpr(pattern=join.str, text=cur.stat, perl=TRUE)
#            
#            checkTrue(join.match != -1)
#            
#            #is it respecting should.ignore
#            
#            ignore.match <- regexpr(pattern="INSERT\\s+OR\\s+IGNORE", text=cur.stat, perl=TRUE)
#            
#            if (tbsl@tab.list[[i]]$should.ignore)
#            {
#                checkTrue(ignore.match != -1)
#            }
#            else
#            {
#                checkTrue(ignore.match == -1)
#            }
#        }
#    }
#}
#
