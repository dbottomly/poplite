setClass(Class="Database", representation=list(tbsl="TableSchemaList", db.file="character"))

setMethod("show", signature("Database"), function(object)
	  {
	    message("Database Instance")
	    message(paste0("File: '", dbFile(object), "'"))
	    show(schema(object))
	  })

setGeneric("schema", def=function(obj,...) standardGeneric("schema"))
setMethod("schema", signature("Database"), function(obj)
	  {
	    return(obj@tbsl)
	  })

setGeneric("dbFile", def=function(obj,...) standardGeneric("dbFile"))
setMethod("dbFile", signature("Database"), function(obj)
	  {
	    return(obj@db.file)
	  })

setGeneric("tables", def=function(obj,...) standardGeneric("tables"))
setMethod("tables", signature("TableSchemaList"), function(obj)
	  {
	    return(schemaNames(obj))
	  })
setMethod("tables", signature("Database"), function(obj)
	  {
	    return(tables(schema(obj)))
	  })

setMethod("columns", signature("Database"), function(obj)
	  {
	    cur.schema <- schema(obj)
	    
	    return(columns(cur.schema))
	  })

Database <- function(tbsl, db.file)
{
    if (class(tbsl) != "TableSchemaList")
    {
	stop("ERROR: tbsl needs to be an instance of class TableSchemaList")
    }
    
    if ((is.character(db.file) && length(db.file) == 1)==F)
    {
	stop("ERROR: db.file needs to be a single path to a file")
    }
    
    #just want to make sure there is an available DB file...somewhat wasteful
    if (file.exists(db.file) == F)
    {
	temp.con <- dbConnect(SQLite(), db.file)
	dbDisconnect(temp.con)
    }
    
    return(new("Database", tbsl=tbsl, db.file=db.file))
}

#still under construction, need to deal with multiple tables and possibly outer joins and such
setGeneric("join", def=function(obj, ...) standardGeneric("join"))
setMethod("join", signature("Database"), function(obj, needed.tables)
	  {
	    if (is.character(needed.tables) == F || (all(needed.tables %in% tables(obj))==F && all(names(needed.tables %in% tables(obj))) == F))
	    {
		stop("ERROR: needed.tables needs to be a character vector corresponding to table names")
	    }
	    
	    if (length(needed.tables) > 1)
	    {
		
		#use the TBSL object to determine how to join the tables and create a temporary table
		if (length(needed.tables) < length(tables(obj)))
		{
		    if (is.null(names(needed.tables)))
		    {
			start.node <- get.starting.point(schema(obj), needed.tables)
		    }else{
			start.node <- get.starting.point(schema(obj), names(needed.tables))
		    }
		    
		    table.path <- get.shortest.query.path(schema(obj), start=start.node, finish=NULL, reverse=F, undirected=T)
		    
		    valid.path <- sapply(table.path, function(x) all(needed.tables %in% x) || (is.null(names(needed.tables)) == F && all(names(needed.tables) %in% x)))
		    
		    if (all(valid.path == F))
		    {
			stop("ERROR: Cannot determine how to join tables, query cannot be carried out")
		    }
		    
		    min.valid.path <- which.min(sapply(table.path[valid.path], length))
		    
		    use.path <- table.path[valid.path][[min.valid.path]]
		}else{
		    if (is.null(names(needed.tables)))
		    {
			use.path <- needed.tables 
		    }else{
			use.path <- names(needed.tables)
		    }
		}
		
		#the joining needs to take into account not just the direct keys from one table to the next but also the necessary
		#keys if one table has already been merged to another as well as any keys that are shared between the tables that
		#were derived from a downstream table
		
		join.cols <- lapply(1:(length(use.path)-1), function(x) {
		    
		    #if x and x + 1 share keys with another table(s)
		    
		    #x.keys <- foreignLocalKeyCols(schema(obj), use.path[x])
		    #xp1.keys <- foreignLocalKeyCols(schema(obj), use.path[x+1])
		    #
		    #common.key.tables <- intersect(names(x.keys), names(xp1.keys))
		    #
		#    if (length(common.key.tables) > 0)
		#    {
		#	browser()
		#	add.keys <- unlist(mapply(function(x,y) intersect(x,y), x.keys[common.key.tables], xp1.keys[common.key.tables]))
		#	
		#    }
		    
		    #maybe it simply needs to be if they share a direct key...
		    
		    common.key <- intersect(directKeys(schema(obj), use.path[x]), directKeys(schema(obj), use.path[x+1]))
		    
		    if(length(common.key) > 0){
			
			add.keys <- common.key
		    }else{
			add.keys <- NULL
		    }
		    
		    #finally the direct keys from one table to the next
		    for.join <- foreignLocalKeyCols(schema(obj), use.path[x], use.path[x+1])
		    if (is.null(for.join))
		    {
			back.join <- foreignLocalKeyCols(schema(obj), use.path[x+1], use.path[x])
			
			if (is.null(back.join))
			{
			      browser()
			    stop("ERROR: Cannot determine join structure")
			}
			else
			{
			    #the as.character has to do with inner_join and company have different semantics based on named versus unnamed
			    return(as.character(append(back.join, add.keys)))
			}
		    }
		    else
		    {
			return(as.character(append(for.join, add.keys)))
		    }
		})
		
		#now using dplyr::inner_join(x,y,by=NULL)
		
		src.db <- src_sqlite(dbFile(obj), create = F)
		
		if (is.null(names(needed.tables)))
		{
		    
		    all.tab <- tbl(src.db, use.path[1])
		
		    use.path <- use.path[-1]
		    
		    for(i in seq_along(use.path))
		    {
			all.tab <- inner_join(all.tab, tbl(src.db,use.path[i]), by=join.cols[[i]])
		    }
		    
		}else{
		    
		    all.tab <- tbl(src.db, use.path[1])
		    
		    if (use.path[1] %in% names(needed.tables))
		    {
			all.tab <- eval(parse(text=paste("select(all.tab, ", needed.tables[use.path[1]], ")")))
			if (all(join.cols[[1]] %in% colnames(all.tab) ==F))
			{
			    diff.cols <- setdiff(join.cols[[1]], colnames(all.tab))
			    all.tab <- tbl(src.db, use.path[1])
			    all.tab <- eval(parse(text=paste("select(all.tab, ", paste(diff.cols, collapse=",") , ",",needed.tables[use.path[1]], ")")))
			}
			
		    }
		    
		    use.path <- use.path[-1]
		    
		    for(i in seq_along(use.path))
		    {
			temp.tab <- tbl(src.db,use.path[i])
			
			if (use.path[i] %in% names(needed.tables))
			{
			    temp.tab <- eval(parse(text=paste("select(temp.tab, ", needed.tables[use.path[i]], ")")))
			    if (all(join.cols[[i]] %in% colnames(temp.tab) ==F))
			    {
				diff.cols <- setdiff(join.cols[[i]], colnames(temp.tab))
				temp.tab <- tbl(src.db,use.path[i])
				temp.tab <- eval(parse(text=paste("select(temp.tab, ", paste(diff.cols, collapse=",") , needed.tables[use.path[i]], ")")))
			    }
			}
			
			all.tab <- inner_join(all.tab, temp.tab, by=join.cols[[i]])
		    }
		    
		    #make sure the final table only includes the requested columns
		    all.tab <- eval(parse(text=paste("select(all.tab, ", paste(needed.tables, collapse=","), ")")))
		}
		
	    }else{
		
		my_db <- src_sqlite(dbFile(obj), create = F)
		
		if (is.null(names(needed.tables))==F)
		{
		    all.tab <- tbl(my_db, names(needed.tables))
		    all.tab <- eval(parse(text=paste("select(all.tab, ", needed.tables, ")")))
		    
		}else{
		    all.tab <- tbl(my_db, needed.tables)
		}
	    }
	    
	    return(all.tab)
	  
	  })


setGeneric("populate", def=function(obj, ...) standardGeneric("populate"))
setMethod("populate", signature("Database"), function(obj, ..., use.tables=NULL, should.debug=FALSE)
	  {
	    db.con <- dbConnect(SQLite(), dbFile(obj))
	    
	    .populate(schema(obj), db.con, ins.vals=list(...), use.tables=use.tables, should.debug=should.debug)
	    
	    invisible(dbDisconnect(db.con))
	  })

.populate <- function(obj, db.con,ins.vals=NULL, use.tables=NULL, should.debug=FALSE)
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
	   
	    dbBegin(db.con)
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
            dbBegin(db.con)
            dbGetPreparedQuery(db.con, insertStatement(db.schema, i), bind.data = bindDataFunction(db.schema, i, ins.vals))
            dbCommit(db.con)
        }
        
    }
}