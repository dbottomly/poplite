
#need to add validitity checks to default.search.cols to below function
valid.TableSchemaList <- function(object)
{
    if (is.null(names(object@tab.list)))
    {
        return("The supplied tab.list needs to have names")
    }
    
    valid.list <- sapply(object@tab.list, function(x)
           {
                if (is.null(names(x)) == TRUE || all(names(x) %in% c( "db.cols","db.schema", "db.constr", "dta.func", "should.ignore", "foreign.keys")) == FALSE)
                {
                    return(FALSE)
                }
                else
                {
                    if (length(x$db.schema) == length(x$db.cols))
                    {
                        if (is.null(x$foreign.keys))
                        {
                            return(TRUE)
                        }
                        else if (class(x$foreign.keys) == "list" && is.null(names(x$foreign.keys)) == FALSE)
                        {
                            return(all(sapply(x$foreign.keys, function(x)
                                   {
                                        return(all(names(x) %in% c("local.keys", "ext.keys")))
                                   })))
                        }
                        else
                        {
                            return(FALSE)
                        }
                    }
                    else
                    {
                        return(FALSE)
                    }
                }
           })
    
    if (all(valid.list) == TRUE)
    {
        return(TRUE)
    }
    else
    {
        return(paste("Invalid input for: ", names(valid.list)[valid.list == FALSE]))
    }
}

setClass(Class="TableSchemaList", representation=list(tab.list="list", search.cols="list"), prototype=prototype(tab.list=list(), search.cols=list()), validity=valid.TableSchemaList)

#need to fix me...
TableSchemaList <- function(tab.list=NULL, search.cols=NULL)
{
    return(new("TableSchemaList", tab.list=tab.list, search.cols=search.cols))
}

setMethod("show", signature("TableSchemaList"), function(object)
          {
                message(paste("TableSchemaList of length", length(object)))
          })

#may need to use BiocGenerics at some point...
#setGeneric("append", def=function(x, values, after) standardGeneric("append"))
setMethod("append", signature("TableSchemaList", "TableSchemaList"), function(x, values, after=length(x))
	  {
		 lengx <- length(x)
		    
		    if (!after) 
			return(new("TableSchemaList", tab.list=c(values@tab.list, x@tab.list)))
		    else if (after >= lengx) 
			return(new("TableSchemaList", tab.list=c(x@tab.list, values@tab.list)))
		    else return(new("TableSchemaList", tab.list=c(x@tab.list[1L:after], values@tab.list, x@tab.list[(after + 1L):lengx])))
		
	  })

#setGeneric("length", def=function(x), standardGeneric("length"))
setMethod("length", signature("TableSchemaList"), function(x)
	  {
		return(length(x@tab.list))
	  })


setMethod("subset", signature("TableSchemaList"), function(x, table.name)
          {
            if (all(table.name %in% names(x@tab.list)) == FALSE)
            {
                stop("ERROR: Only valid names can be used for subsetting")
            }
            
            return(new("TableSchemaList", tab.list=x@tab.list[table.name]))
          })

setGeneric("makeSchemaFromData", def=function(obj, ...) standardGeneric("makeSchemaFromData"))
setMethod("makeSchemaFromData", signature("data.frame"), function(obj, name=NULL, primary.cols=NULL)
          {
            if (missing(name) || is.null(name) || is.na(name))
            {
                stop("ERROR: Please supply a name for the table")
            }
	    
	    if (is.null(names(obj)))
	    {
		stop("ERROR: obj needs to have column names")
	    }
	    
	    if (valid.db.names(names(obj)) == F)
	    {
		stop("ERROR: The names of the supplied data.frame need to be modified for the database see correct.df.names")
	    }
            
            cur.list <- list(db.cols=character(0), db.schema=character(0), db.constr="", dta.func= eval(parse(text=paste0("function(x) x[['",name,"']]"))), should.ignore=T, foreign.keys=NULL)
            
            if (missing(primary.cols) || is.null(primary.cols) || is.na(primary.cols))
            {
                cur.list$db.constr <- ""
                cur.list$db.cols <- paste0(name, "_ind")
                cur.list$db.schema <- "INTEGER PRIMARY KEY AUTOINCREMENT"
            }
            else if (is.character(primary.cols) && is.null(names(obj)) == F && all(primary.cols %in% names(obj)))
            {
                cur.list$db.constr <- paste0("CONSTRAINT ", name, "_idx UNIQUE (", paste(primary.cols, collapse=",") ,")")
            }
            else
            {
                stop("ERROR: primary.cols needs to be NULL or a character vector corresponding to the names of obj")
            }
            
            cur.list$db.cols <- append(cur.list$db.cols, names(obj))
            
            cur.list$db.schema <- append(cur.list$db.schema, determine.db.types(obj))
            
            tab.list <- list(cur.list)
            names(tab.list) <- name
            
            return(new("TableSchemaList", tab.list=tab.list))
          })

correct.df.names <- function(dta)
{
    names(dta) <- make.db.names.default(names(dta))
    return(dta)
}

valid.db.names <- function(inp.names)
{
    conv.names <- make.db.names.default(inp.names)

    if (length(setdiff(inp.names, conv.names)) > 0)
    {
	return(FALSE)
    }
    else
    {
	return(TRUE)
    }
}

character.to.type <- function(val.class)
{
    char.val <- as.character(val.class)
    
    exist.nas <- sum(is.na(char.val))
    is.numeric.type <- suppressWarnings(as.numeric(char.val))
    
    if (sum(is.na(is.numeric.type)) > exist.nas)
    {
        return("TEXT")
    }
    else
    {
        if(any(grepl("\\d*\\.\\d*",char.val)))
        {
            return("NUMERIC")
        }
        else
        {
            return("INTEGER")
        }
    }
}

basic.integer <- function(x)
{
    return("INTEGER")
}


basic.text <- function(x)
{
    return("TEXT")
}

determine.db.types <- function(dta)
{
    return(sapply(names(dta), function(x)
           {
                return(switch(class(dta[,x]), character=basic.text, factor=character.to.type,
                    numeric=character.to.type, integer=basic.integer, basic.text)(dta[,x]))
           }))
}


return.element <- function(use.obj, name)
{
    return(sapply(use.obj@tab.list, "[[", name))
}

setGeneric("populate", def=function(obj, ...) standardGeneric("populate"))
setMethod("populate", signature("TableSchemaList"), function(obj, db.con,ins.vals=NULL, use.tables=NULL, should.debug=FALSE)
{
    db.schema <- obj
    
    if (class(db.con) != "SQLiteConnection")
    {
        stop("ERROR: db.con needs to be of class SQLiteConnection")
    }
    
    if (missing(ins.vals) || is.null(ins.vals))
    {
        stop("ERROR: ins.vals cannot be missing or NULL")
    }
	
    if (missing(use.tables) || is.null(use.tables) || is.na(use.tables))
    {
        use.tables <- schemaNames(db.schema)
    }
    else if (all(use.tables %in% schemaNames(db.schema)) == FALSE)
    {
        stop("ERROR: Invalid values for use.tables")
    }
    
    #schemaNames should be arranged in the order of population
    for(i in use.tables)
    {
        message(paste("Starting", i))
        #if table doesn't exist, then create it
        if (dbExistsTable(db.con, tableName(db.schema, i, mode="normal")) == FALSE)
        {
            if (should.debug) message("Creating database table")
            if (should.debug) message(createTable(db.schema, i, mode="normal"))
            dbGetQuery(db.con, createTable(db.schema, i, mode="normal"))
        }
        
        #then merge with existing databases as necessary

        if (shouldMerge(db.schema, i))
        {
            if (should.debug) message("Creating temporary table for merging")
            
            if (dbExistsTable(db.con, tableName(db.schema, i, mode="merge")))
            {
                stop("ERROR: Temporary tables should not exist prior to this loop")
            }
            
            if (should.debug) message(createTable(db.schema, i, mode="merge"))
            dbGetQuery(db.con, createTable(db.schema, i, mode="merge"))
            
            if (should.debug) message("Adding to temporary table")
            if (should.debug) message(insertStatement(db.schema, i, mode="merge"))
            #first add the data to temporary database
            dbBeginTransaction(db.con)
            dbGetPreparedQuery(db.con, insertStatement(db.schema, i, mode="merge"), bind.data = bindDataFunction(db.schema, i, ins.vals, mode="merge"))
            dbCommit(db.con)
            
            #merge from temporary into main table
            if (should.debug) message("Merging with existing table(s)")
            if (should.debug) message(mergeStatement(db.schema, i))
            dbGetQuery(db.con, mergeStatement(db.schema, i))
            
            #then also drop intermediate tables
            if (should.debug) message("Removing temporary table")
            if (should.debug) message(paste("DROP TABLE", tableName(db.schema, i, mode="merge")))
            dbGetQuery(db.con, paste("DROP TABLE", tableName(db.schema, i, mode="merge")))
        }else
        {
            if (should.debug) message("Adding to database table")
            if (should.debug) message(insertStatement(db.schema, i))
            #add the data to database
            dbBeginTransaction(db.con)
            dbGetPreparedQuery(db.con, insertStatement(db.schema, i), bind.data = bindDataFunction(db.schema, i, ins.vals))
            dbCommit(db.con)
        }
        
    }
})

setGeneric("searchTables", def=function(obj, ...) standardGeneric("searchTables"))
setMethod("searchTables", signature("TableSchemaList"), function(obj, name)
          {
            return(sapply(obj@search.cols[name], "[[", "table"))
          })

setGeneric("searchCols", def=function(obj, ...) standardGeneric("searchCols"))
setMethod("searchCols", signature("TableSchemaList"), function(obj, name)
          {
            return(sapply(obj@search.cols[name], "[[", "column"))
          })

setGeneric("searchDict", def=function(obj, ...) standardGeneric("searchDict"))
setMethod("searchDict", signature("TableSchemaList"), function(obj, name, value=NULL)
          {
            if (missing(value) || is.null(value) || is.na(value))
            {
                return(lapply(obj@search.cols[name], "[[", "dict"))
            }
            else
            {
                return(sapply(lapply(obj@search.cols[name], "[[", "dict"), "[", value))
            }
          })

#need to go through, either here or in the object validation function to ensure that the expected foreign keys match up to the observed
setGeneric("foreignExtKeyCols", def=function(obj, ...) standardGeneric("foreignExtKeyCols"))
setMethod("foreignExtKeyCols", signature("TableSchemaList"), function(obj, table.name)
          {
            as.character(sapply(return.element(obj, "foreign.keys")[table.name], function(x)
                   {
                        as.character(unlist(sapply(names(x), function(y)
                               {
                                    return(x[[y]]$ext.keys)
                               })))
                   }))
          })

setGeneric("foreignLocalKeyCols", def=function(obj, ...) standardGeneric("foreignLocalKeyCols"))
setMethod("foreignLocalKeyCols", signature("TableSchemaList"), function(obj, table.name)
          {
            as.character(sapply(return.element(obj, "foreign.keys")[table.name], function(x)
                   {
                        as.character(unlist(sapply(names(x), function(y)
                               {
                                    return(x[[y]]$local.keys)
                               })))
                   }))
          })

setGeneric("foreignExtKeySchema", def=function(obj, ...) standardGeneric("foreignExtKeySchema"))
setMethod("foreignExtKeySchema", signature("TableSchemaList"), function(obj, table.name)
          {
            as.character(sapply(return.element(obj, "foreign.keys")[table.name], function(x)
                   {
                        as.character(unlist(sapply(names(x), function(y)
                               {
                                    colSchema(obj, y, mode="normal")[match(x[[y]]$ext.keys, colNames(obj, y, mode="normal"))]
                               })))
                   }))
          })

setGeneric("bindDataFunction", def=function(obj, ...) standardGeneric("bindDataFunction"))
setMethod("bindDataFunction", signature("TableSchemaList"), function(obj, table.name, bind.vals, mode=c("normal", "merge"))
        {
            table.mode <- match.arg(mode)
            
            vcf.dta <- return.element(subset(obj, table.name), "dta.func")[[1]](bind.vals)
            
            cur.cols <- colNames(obj, table.name, mode=table.mode)
            cur.schema <- colSchema(obj, table.name, mode=table.mode)
            
            #don't need to supply columns which are autoincremented, they will be automatically added to the data.frame
            auto.col <- cur.cols[cur.schema == "INTEGER PRIMARY KEY AUTOINCREMENT"]
            #stopifnot(length(auto.col) == 1)
            
            diff.cols <- setdiff(cur.cols, colnames(vcf.dta))
            
            if (length(diff.cols) == 0)
            {
                return(vcf.dta[,cur.cols])
            }
            else if (length(auto.col) == 1 && length(diff.cols) == 1 && diff.cols == auto.col)
            {
                temp.vcf.dta <- cbind(vcf.dta, NA_integer_)
                names(temp.vcf.dta) <- c(names(vcf.dta), auto.col)
                return(temp.vcf.dta[,cur.cols])
            }
            else
            {
                nf.cols <- setdiff(cur.cols, colnames(vcf.dta))
                stop(paste("ERROR: Cannot find column(s)", paste(nf.cols, collapse=",")))
            }
            
        })

setGeneric("shouldIgnore", def=function(obj, ...) standardGeneric("shouldIgnore"))
setMethod("shouldIgnore", signature("TableSchemaList"), function(obj, table.name)
        {
            return(return.element(subset(obj, table.name), "should.ignore"))
        })

setGeneric("shouldMerge", def=function(obj, ...) standardGeneric("shouldMerge"))
setMethod("shouldMerge", signature("TableSchemaList"), function(obj, table.name=NULL)
        {
            if (missing(table.name) || is.null(table.name))
            {
                sub.obj <- obj
            }
            else
            {
                sub.obj <- subset(obj, table.name)
            }
            
            return(any(sapply(return.element(sub.obj, "foreign.keys"), is.null) == FALSE))
        })

setGeneric("schemaNames", def=function(obj, ...) standardGeneric("schemaNames"))
setMethod("schemaNames", signature("TableSchemaList"), function(obj)
        {
            names(obj@tab.list)
        })

setGeneric("tableConstr", def=function(obj, ...) standardGeneric("tableConstr"))
setMethod("tableConstr", signature("TableSchemaList"), function(obj, table.name, mode=c("normal", "merge"))
        {
            table.mode <- match.arg(mode)
            
            ret.el <- return.element(subset(obj, table.name), "db.constr")
            
            if (is.null(ret.el) || is.na(ret.el) || table.mode == "merge")
            {
                return("")
            }
            else
            {
                return(ret.el)
            }
        })

setGeneric("tableName", def=function(obj, ...) standardGeneric("tableName"))
setMethod("tableName", signature("TableSchemaList"), function(obj, table.name, mode=c("normal", "merge"))
          {
                table.mode <- match.arg(mode)
            
                if (table.name %in% names(obj@tab.list) == FALSE)
                {
                    stop("ERROR: Invalid table.name supplied")
                }
                
                return(switch(table.mode, normal=table.name, merge=paste(table.name, "temp", sep="_")))
          })

setGeneric("colNames", def=function(obj, ...) standardGeneric("colNames"))
setMethod("colNames", signature("TableSchemaList"), function(obj, table.name, mode=c("normal", "merge"))
          {
                table.mode <- match.arg(mode)
                sub.obj <- subset(obj, table.name)
                base.cols <- as.character(return.element(sub.obj, "db.cols"))
                base.schema <- as.character(return.element(sub.obj, "db.schema"))
                foreign.cols <- foreignExtKeyCols(obj, table.name)
                
                #also remove the columns that will be present in the final table but not part of the initial table
                rm.cols <- foreignLocalKeyCols(obj, table.name)
                
                return(switch(table.mode, normal=base.cols, merge=c(foreign.cols[foreign.cols %in% rm.cols == FALSE], base.cols[base.schema != "INTEGER PRIMARY KEY AUTOINCREMENT" & base.cols %in% rm.cols == FALSE])))
          })

setGeneric("colSchema", def=function(obj, ...) standardGeneric("colSchema"))
setMethod("colSchema", signature("TableSchemaList"), function(obj, table.name, mode=c("normal", "merge"))
          {
                table.mode <- match.arg(mode)
                sub.obj <- subset(obj, table.name)
                base.schema <- as.character(return.element(sub.obj, "db.schema"))
                base.cols <- as.character(return.element(sub.obj, "db.cols"))
                foreign.schema <- foreignExtKeySchema(obj, table.name)
                foreign.cols <- foreignExtKeyCols(obj, table.name)
                
                #also remove the columns that will be present in the final table but not part of the initial table
                rm.cols <- foreignLocalKeyCols(obj, table.name)
                rm.schema <- base.cols %in% rm.cols
                
                return(switch(table.mode, normal=base.schema, merge=c(foreign.schema[foreign.cols %in% rm.cols == FALSE], base.schema[base.schema != "INTEGER PRIMARY KEY AUTOINCREMENT" & rm.schema == FALSE])))
          })

setGeneric("createTable", def=function(obj, ...) standardGeneric("createTable"))
setMethod("createTable", signature("TableSchemaList"), function(obj, table.name, mode=c("normal", "merge"))
          {
                table.mode <- match.arg(mode)
                if (shouldMerge(obj, table.name) == TRUE && table.mode == "merge")
                {
                    temp.str <- "TEMPORARY"
                }
                else if (table.mode == "normal")
                {
                    temp.str <- ""
                }
                else
                {
                    stop("ERROR: Cannot generate statement with mode set to 'merge' and a NULL foreign.key element")
                }
                
                use.cols <- colNames(obj, table.name, mode=table.mode)
                use.schema <- colSchema(obj, table.name, mode=table.mode)
                
                tab.constr <- tableConstr(obj, table.name, mode=table.mode)
                tab.constr <- ifelse(tab.constr == "", tab.constr, paste0(",", tab.constr))
                
                return(paste("CREATE",temp.str,"TABLE", tableName(obj, table.name, table.mode), "(", paste(paste(use.cols, use.schema), collapse=","), tab.constr, ")"))
          })

setGeneric("mergeStatement", def=function(obj, ...) standardGeneric("mergeStatement"))
setMethod("mergeStatement", signature("TableSchemaList"), function(obj, table.name)
          {
                #currently, probably the temporary table
                cur.db <- tableName(obj, table.name, mode="merge")
                #table trying to create
                target.db <- tableName(obj, table.name, mode="normal")
                
		target.cols <- colNames(obj, table.name, mode="normal")
                target.schema <- colSchema(obj, table.name, mode="normal")
                #remove the autoincrement column first
                target.cols <- target.cols[target.schema != "INTEGER PRIMARY KEY AUTOINCREMENT"]
		
                #create the join statement using the foreign.keys slot
                
                fk <- return.element(obj, "foreign.keys")[[table.name]]
                
                if (is.null(fk))
                {
                    stop("ERROR: Cannot generate statement if the foreign key element is NULL")
                }
                
                keys <- sapply(names(fk), function(y)
                       {
                            return(paste(fk[[y]]$ext.keys, collapse=","))
                       })
		
		
                join.statement <- paste(paste("JOIN", names(keys), "USING", paste0("(", keys,")")), collapse=" ")
		
		#in case columns besides the keys are duplicated, make sure the select statement refers to the appropriate table
                
		names(target.cols) <- rep(cur.db, length(target.cols))
		
		for(i in names(fk))
		{
		    join.tab.cols <- target.cols %in% fk[[i]]$local.keys
		    
		    if (any(join.tab.cols))
		    {
			names(target.cols)[join.tab.cols] <- i
		    }
		}
		
		plain.targs <- paste(target.cols, collapse=",")
		
		
		paste.targs <- paste(paste(names(target.cols), target.cols, sep="."), collapse=",")
		
                if (shouldIgnore(obj, table.name))
                {
                    ignore.str <- "OR IGNORE"
                }
                else
                {
                    ignore.str <- ""
                }
                
                return(paste("INSERT",ignore.str,"INTO", target.db, "(", plain.targs,") SELECT", paste.targs,"FROM", cur.db , join.statement))
          })

setGeneric("insertStatement", def=function(obj, ...) standardGeneric("insertStatement"))
setMethod("insertStatement", signature("TableSchemaList"), function(obj, table.name, mode=c("normal", "merge"))
          {
                table.mode <- match.arg(mode)
                
                if (shouldMerge(obj, table.name) == FALSE && table.mode == "merge")
                {
                    stop("ERROR: Cannot run statement with mode 'merge' and a NULL foreign.key element")
                }
                
                use.cols <- colNames(obj, table.name, mode=table.mode)
                
                if (shouldIgnore(obj, table.name))
                {
                    ignore.str <- "OR IGNORE"
                }
                else
                {
                    ignore.str <- ""
                }
                
                return(paste("INSERT",ignore.str,"INTO", tableName(obj, table.name, table.mode), "VALUES (", paste(paste0(":", use.cols), collapse=","), ")"))
          })

get.model.side <- function(form, side=c("left", "right"))
{    
    side <-  match.arg(side)
    
    char.form <- as.character(form)
    
    if (length(char.form) == 3)
    {
        use.ind <- switch(side, right=3, left=2)
        
        split.lhs <- strsplit(char.form[use.ind], "\\s+\\+\\s+")[[1]]
        return(split.lhs)
    }
    else
    {
        stop("ERROR: Unexpected parsing for input formula")
    }
}



rhs <- function(form)
{
    get.model.side(form, "right")
}

lhs <- function(form)
{
    get.model.side(form, "left")
}

setGeneric("relationship<-", def=function(obj, from, to, value) standardGeneric("relationship<-"))
setReplaceMethod("relationship", signature("TableSchemaList"), function(obj, from, to, value)
                 {
                   
                    #check to be sure 'from' comes before 'to'
                    
                    from.pos <- which(names(obj@tab.list) == from)
                    to.pos <-  which(names(obj@tab.list) == to)
                    
                    if (to.pos <= from.pos)
                    {
                        stop("ERROR: The 'from' table needs to be specified before the 'to' table")
                    }
                    
                    cur.lhs <- lhs(value)
                    cur.rhs <- rhs(value)
                    
                    if (cur.lhs == ".")
                    {
                        #check to see if there is an auto-incremented primary key for from
                        which.prim <- which(obj@tab.list[[from]]$db.schema == "INTEGER PRIMARY KEY AUTOINCREMENT")
                        if (length(which.prim) == 1)
                        {
                            cur.lhs <- obj@tab.list[[from]]$db.cols[which.prim]
                        }
                        else
                        {
                            stop("ERROR: Can't specify '.' in the formula when a autoincremented primary key is not available")
                        }
                    }
                    else
                    {
                        #check to be sure that the values are in from
                        if (all(cur.lhs %in% obj@tab.list[[from]]$db.cols) == F)
                        {
                            stop("ERROR: All values on the lhs of the formula need to be in the 'from' table's columns")
                        }
                    }
                    
                    #check to be sure that the values are in to
                    if (all(cur.rhs %in% obj@tab.list[[to]]$db.cols) == F)
                    {
                        stop("ERROR: All values on the rhs of the formula need to be in the 'to' table's columns")
                    }
                    
                    #additionally, they all need to be in from's as well
                    
                    if (all(cur.rhs %in% obj@tab.list[[from]]$db.cols) == F)
                    {
                        stop("ERROR: All values on the rhs of the formula need to be in the 'from' table's columns")
                    }
                
                    #if everything looks good, then add in the relationships to the to table
                    
                    use.fk <- list(list(local.keys=cur.lhs, ext.keys=cur.rhs))
                    names(use.fk) <- from
                    
                    if (is.null(obj@tab.list[[to]]$foreign.keys))
                    {
                        obj@tab.list[[to]]$foreign.keys <- use.fk
                    }
                    else
                    {
                        obj@tab.list[[to]]$foreign.keys <- append(obj@tab.list[to]$foreign.keys, use.fk)
                    }
                    
                    #also modify db.cols and db.schema to reflect the new keys/relationships
                    
                    which.rhs <- which(obj@tab.list[[to]]$db.cols %in% cur.rhs)
                    obj@tab.list[[to]]$db.cols <- obj@tab.list[[to]]$db.cols[-which.rhs]
                    obj@tab.list[[to]]$db.schema <- obj@tab.list[[to]]$db.schema[-which.rhs] 
                    
                    obj@tab.list[[to]]$db.cols <- append(obj@tab.list[[to]]$db.cols, cur.lhs)
                    
                    #add in what the schema is in from, cleaning a little for integer autoincrement
                    
                    curm <- match(cur.lhs, obj@tab.list[[from]]$db.cols)
                    
                    obj@tab.list[[to]]$db.schema <- append(obj@tab.list[[to]]$db.schema, sapply(strsplit(obj@tab.list[[from]]$db.schema[curm], "\\s+"), "[[", 1))
                    
                    validObject(obj)
                    return(obj)
                 })
