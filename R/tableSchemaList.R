
#interfaces with igraph

#still under construction in test_poplite
tsl.to.graph <- function(tsl)
{
    graph.list <- lapply(tsl@tab.list, function(x)
           {
                cur.edges <- unlist(lapply(x$foreign.keys, "[[", "local.keys"))
                if (is.null(cur.edges))
                {
                    return(NULL)
                }
                else
                {
                    common.col <- intersect(x$db.cols, cur.edges)
                    use.fk <- sapply(x$foreign.keys, function(x) x$local.keys %in% common.col)
                    return(names(use.fk)[use.fk == TRUE])
                }
           })
    
    graph.comp.list <- graph.list[sapply(graph.list, is.null)==F]
    
    return(graph.data.frame(stack(graph.comp.list)))
}

get.starting.point <- function(tbsl, use.tables)
{
    tsl.graph <- tsl.to.graph(tbsl)
    sp.mat <- shortest.paths(tsl.graph, mode="all")
    sp.mat[is.infinite(sp.mat)] <- 0
    diag(sp.mat) <- NA
    
    sp.mat <- sp.mat[use.tables, use.tables, drop=F]
    
    is.valid <- apply(sp.mat, 1, function(x) all(na.omit(x) > 0))
    
    valid.mat <- sp.mat[is.valid,,drop=F]
    
    dists <- apply(valid.mat, 1, function(x) sum(na.omit(x)))
    
    #assuming that the furthest table is at one end of the query path and 'should' make a good start point
    return(names(dists)[which.max(dists)])
}

get.shortest.query.path <- function(tbsl, start=NULL, finish=NULL, reverse=TRUE, undirected=TRUE)
{   
    tsl.graph <- tsl.to.graph(tbsl)
    
    if (missing(finish) || is.null(finish) || is.na(finish))
    {
        finish <- V(tsl.graph)
    }
    
    if (undirected)
    {
        use.mode <- "all"
    }else{
        use.mode <- "out"
    }
    
    table.path <- get.shortest.paths(graph=tsl.graph,from=start,to=finish, mode = use.mode, weights = NULL, output="vpath",predecessors = FALSE, inbound.edges = FALSE)
    #do it backwards as that is how we want to merge it with the pd tables
    
    mask.query <- lapply(table.path$vpath, function(x)
			 {
			    if (reverse==TRUE)
			    {
				return(rev(V(tsl.graph)[x]$name))
			    }
			    else
			    {
				return(V(tsl.graph)[x]$name)
			    }
			 })
    
    return(mask.query)
}





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

setClass(Class="TableSchemaList", representation=list(tab.list="list"), prototype=prototype(tab.list=list()), validity=valid.TableSchemaList)

#need to fix me...
TableSchemaList <- function(tab.list=NULL)
{
    return(new("TableSchemaList", tab.list=tab.list))
}

setMethod("show", signature("TableSchemaList"), function(object)
          {
                message(paste("TableSchemaList containing", length(object), "tables"))
		
		if (length(object) > 0)
		{
		    for (i in 1:length(object))
		    {
			num.cols <- length(object@tab.list[[i]]$db.cols)
			col.val <- ifelse(num.cols == 1, "Column", "Columns")
			message(paste("   ", "-", names(object@tab.list)[i], "(", num.cols, col.val,")"))
			if (is.null(object@tab.list[[i]]$foreign.keys) ==F)
			{
			    for (j in names(object@tab.list[[i]]$foreign.keys))
			    {
				all.loc.keys <- object@tab.list[[i]]$foreign.keys[[j]]$local.keys
				
				#check if the loc.keys are still retained in the table or only used for merging purposes
				
				loc.keys <- all.loc.keys[all.loc.keys %in% object@tab.list[[i]]$db.cols]
				
				if (length(loc.keys) > 0)
				{
				     if (length(loc.keys) == 1)
				    {
					message(paste("      - Column", loc.keys, "is derived from table", j))
				    }else{
					message(paste("      - Columns", paste(loc.keys, collapse=","), "are derived from table", j))
				    }
				}
    
			    }
			}
		    }
		}
		
		
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

makeSchemaFromData <- function(tab.df, name=NULL, primary.cols=NULL)
{
  if (missing(name) || is.null(name) || is.na(name))
  {
      stop("ERROR: Please supply a name for the table")
  }
  
  if (class(tab.df) != "data.frame")
    {
	stop("ERROR: tab.df needs to be a data.frame")
    }
  
  if (is.null(names(tab.df)))
  {
      stop("ERROR: tab.df needs to have column names")
  }
  
  if (valid.db.names(names(tab.df)) == F)
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
  else if (is.character(primary.cols) && is.null(names(tab.df)) == F && all(primary.cols %in% names(tab.df)))
  {
      cur.list$db.constr <- paste0("CONSTRAINT ", name, "_idx UNIQUE (", paste(primary.cols, collapse=",") ,")")
  }
  else
  {
      stop("ERROR: primary.cols needs to be NULL or a character vector corresponding to the names of tab.df")
  }
  
  cur.list$db.cols <- append(cur.list$db.cols, names(tab.df))
  
  cur.list$db.schema <- append(cur.list$db.schema, determine.db.types(tab.df))
  
  tab.list <- list(cur.list)
  names(tab.list) <- name
  
  return(new("TableSchemaList", tab.list=tab.list))
}

correct.df.names <- function(tab.df)
{
    names(tab.df) <- make.db.names.default(names(tab.df))
    return(tab.df)
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


setGeneric("columns", def=function(obj,...) standardGeneric("columns"))
setMethod("columns", signature("TableSchemaList"), function(obj)
	  {
	    
	    ret.list <- lapply(schemaNames(obj), function(x)
		   {
			colNames(obj, x)
		   })
	    
	    names(ret.list) <- schemaNames(obj)
	    
	    return(ret.list)
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
		    
		    valid.path <- sapply(table.path, function(x) all(needed.tables %in% x || all(names(needed.tables) %in% x)))
		    
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
		#keys if one table has already been merged to another...
		join.cols <- lapply(1:(length(use.path)-1), function(x) {
		    
		    if (x > 1)
		    {
			next.path.keys <- foreignLocalKeyCols(schema(obj), use.path[x+1], use.path[1:(x-1)])
			if (length(next.path.keys) > 0)
			{
			    add.keys <- unlist(next.path.keys)
			}
		    }else{
			add.keys <- NULL
		    }
		    
		    for.join <- foreignLocalKeyCols(schema(obj), use.path[x], use.path[x+1])
		    if (is.null(for.join))
		    {
			back.join <- foreignLocalKeyCols(schema(obj), use.path[x+1], use.path[x])
			
			if (is.null(back.join))
			{
			    stop("ERROR: Cannot determine join structure")
			}
			else
			{
			    return(append(back.join, add.keys))
			}
		    }
		    else
		    {
			return(append(for.join, add.keys))
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

#Went with an S3 method here and for select for the S4 Database class to go with the S3 generics in dplyr
filter_.Database <- function(.data, ...,.dots)
	  {
	    #was this, need to change to below, due to changes to dplyr
	    #taken from the internal code of dplyr, the dots() function
	    #use.expr <- eval(substitute(alist(...)))
	    
	    use.expr <- .dots
	    
	    if (length(use.expr) != 1)
	    {
		stop("ERROR: Please supply a single statement which would result in a logical vector")
	    }
	    
	    .get.var.names <- function(x)
	    {
		if (length(x) == 1)
		{
		    return(as.character(x))
		}else{
		    which.calls = sapply(x, class)
		    if (any(which.calls %in% c('call', '(')))
		    {
			return(lapply(x[which.calls %in% c('call', '(')], .get.var.names))
		    }else{
			return(as.character(x[[2]]))
		    }
		    
		}
	    }
	    
	    needed.vars <- unlist(.get.var.names(use.expr[[1]]))
	    
	    #figure out which tables the requested variables are in
	    
	    parse.tables <- get.tables.from.vars(needed.vars)
	    
	    not.prov.tabs <- sapply(parse.tables, is.null)
	    
	    if (any(not.prov.tabs))
	    {
		col.to.tab <- stack(columns(.data))
		
		np.tabs <- lapply(needed.vars[not.prov.tabs], function(x)
				  {
					found.tabs <- as.character(col.to.tab$ind[col.to.tab$values %in% x])
				  })
		
		np.tabs <- append(np.tabs, parse.tables[not.prov.tabs == F])
		
		np.tab.len <- sapply(np.tabs, length)
		
		if(any(np.tab.len == 0))
		{
		    stop(paste("ERROR: Cannot find corresponding table for", paste(needed.vars[np.tab.len == 0],collapse=",")))
		}
		else if (any(np.tab.len > 1))
		{
		    #if all other columns match one specific table, and the multi-mapping col does too, then use that table
		    ##otherwise throw an error
		    
		    unique.tabs <- np.tabs[np.tab.len == 1]
		    
		    conc.tab.list <- lapply(np.tabs[np.tab.len > 1], function(x) x[x %in% unlist(unique.tabs)])
		    conc.list.len <- sapply(conc.tab.list, length)
		    
		    if (all(length(conc.list.len) == 1) && length(unique(unlist(unique.tabs))) == 1)
		    {
			#as everybody agrees on a given table
			np.tabs <- unique.tabs[1]
			
		    }else{
			stop("ERROR: Cannot uniquely map columns to table, try using: tableX.columnY")
		    }
		    
		}
		
		needed.tables <- unique(unlist(np.tabs))
		
	    }else{
		#names are provided for all columns
		needed.tables <- unique(unlist(parse.tables))
	    }
	    
	    my_db_tbl <- join(.data, needed.tables)
	    
	    #carry out the filter method of dplyr on the temporary or otherwise table
	    
	    cur.stat <- paste(deparse(use.expr[[1]]$expr), collapse=" ")
	    
	    for(i in needed.tables)
	    {
		cur.stat <- gsub(paste0(i, "."), "", cur.stat)
	    }
	   
	    use.statement <- paste("filter(my_db_tbl,", cur.stat, ")")
	    
	    return(eval(parse(text=use.statement)))
    }

select <- function(.data,..., .tables=NULL)
{   
    use.dots <- lazy_dots(...)
    
    if ((missing(.tables) || is.null(.tables) || is.na(.tables))==F && class(.data) == "Database")
    {
	use.dots <- append(use.dots, list(.tables=.tables))
    }
    
    select_(.data, .dots = use.dots)
}

select_.Database <- function(.data, ..., .dots)
{
    #check to see if .tables is part of .dots
    
    if('.tables' %in% names(.dots))
    {
	.tables <- .dots$.tables
	#.tables <- lazy_eval(.dots$.tables)
	.dots <- .dots[-which(names(.dots) == ".tables")]
    }else{
	.tables <- NULL
    }
    
    use.expr <- .dots
    
    if (is.null(.tables) == F)
    {
	if (is.character(.tables) && length(.tables) >= 1 && all(.tables %in% tables(.data)))
	{
	   use.tables <- .tables
	}
	else
	{
	    stop("ERROR: .tables needs to be a vector of table names use 'tables(.data)' for a listing")
	}
	
	clean.cols <- sapply(use.expr, function(x) deparse(x$expr))
	
	if (length(clean.cols) > 1)
	{
	    stop(".tables can only be used with at most one select statement, use the dot notation: e.g. 'tableX.columnY'")
	}else if (length(clean.cols) == 1)
	{
	    temp.table <- use.tables
	    use.tables <- clean.cols
	    names(use.tables) <- temp.table
	}
	
    }else if (length(use.expr) == 0 && is.null(.tables))
    {
	stop("ERROR: Please either supply desired columns (columns(.data)) or specify valid table(s) in .tables ('tables(.data)')")
    }else{
	#attempt to figure out what the tables are from the specified columns...
	use.col.list <- lapply(use.expr, function(x)
			   {
				expr.only <- x$expr
				if (length(expr.only) == 1)
				{
				    return(as.character(expr.only))
				}else if (length(expr.only) == 3 && expr.only[[1]] == ":")
				{
				    return(sapply(expr.only[2:3], as.character))
				}else{
				    stop("ERROR: Accepted expression are of the form 'column' or 'column1':'columnN'")
				}
			   })
	
	inp.tab.list <- get.tables.from.vars(use.col.list)
	
	#where the nulls are non-input tables
	not.sup.tab <- sapply(inp.tab.list, function(x) all(is.null(x)))
	
	clean.cols <- sapply(1:length(use.expr), function(x)
				 {
				    if (is.null(inp.tab.list[[x]])==F)
				    {
					return(gsub(paste0(inp.tab.list[[x]], "\\."), "", deparse(use.expr[[x]]$expr)))
				    }else{
					return(deparse(use.expr[[x]]$expr))
				    }
				    
				 })
	
	if(any(not.sup.tab))
	{
	    tab.cols <- columns(.data)
	    
	    #if no tables are specified, need to first try to find a table which contains all the columns
	    tab.ord.mat <- sapply(tab.cols, function(x) sapply(use.col.list[not.sup.tab], function(y) {
		
		temp.match.rank <- rank(match(y, x), na.last=NA)
		
		if (length(temp.match.rank) > 0 && all(temp.match.rank == seq_along(y)))
		{
		    return(T)
		}else{
		    return(F)
		}
	    }))
	    
	    if (is.matrix(tab.ord.mat) == F)
	    {
		tab.ord.mat <- matrix(tab.ord.mat, nrow=1, ncol=length(tab.ord.mat), dimnames=list(NULL, names(tab.cols)))
	    }
	    
	    if (all(apply(tab.ord.mat, 1, function(x) sum(x) == 1)))
	    {
		sel.tabs <- apply(tab.ord.mat, 2, which)
		
		which.to.use <- sapply(sel.tabs, length) > 0
		
		unl.tabs <- unlist(sel.tabs[which.to.use])
		
		use.tables <- clean.cols[not.sup.tab][unl.tabs]
		
		names(use.tables) <- names(unl.tabs)
		
	    }else{
		#otherwise the columns would need to be contiguous between multiple tables
		##probably not going to be easy for user to specify, will throw an error for now...
		
		not.contig <- apply(tab.ord.mat, 1, any) == F
		
		if (any(not.contig))
		{
		    stop(paste("ERROR: Column(s):",paste(sapply(use.expr[not.sup.tab][not.contig], function(x) deparse(x$expr)), collapse=","), "are not contiguous in any table.  Try specifying a table: e.g. 'tableX.columnY'"))
		}else{
		    
		    multi.tab <- apply(tab.ord.mat, 1, function(x) sum(x) > 1)
		    
		    stop(paste("ERROR: Column(s):",paste(sapply(use.expr[not.sup.tab][multi.tab], function(x) deparse(x$expr)), collapse=","), "are found in multiple tables try specifying a table: 'tableX.columnY'"))
		}
		
	    }
	    
	    if(any(not.sup.tab==F))
	    {
		#a mix of specified and non-specified
		##so add in the additional tables, if the columns exist...
		
		if (any(sapply(inp.tab.list[not.sup.tab==F], length) != 1))
		{
		    stop("ERROR: Only a single table should be specified per statement.")
		}
		
		use.tabs <- sapply(inp.tab.list[not.sup.tab==F], "[", 1)
		
		valid.cols <- mapply(function(cols, tabs, cur.tab)
		       {
			    strip.cols <- gsub(paste0(cur.tab, "\\."), "", cols)
			    
			    temp.match.rank <- rank(match(strip.cols, tabs), na.last=NA)
		
			    if (length(temp.match.rank) > 0 && all(temp.match.rank == seq_along(cols)))
			    {
				return(T)
			    }else{
				return(F)
			    }
		       }, use.col.list[not.sup.tab==F], tab.cols[use.tabs], cur.tab=use.tabs)
		
		
		if (all(valid.cols) == F)
		{
		    stop(paste("ERROR: Provided column(s):", paste(sapply(use.expr[not.sup.tab==F][valid.cols==F], function(x) deparse(x$expr)), collapse=","), "could not be found in the specified tables"))
		}
		
		new.tab.list <- clean.cols[not.sup.tab==F]
		names(new.tab.list) <- use.tabs
		
		use.tables <- append(use.tables, new.tab.list)
	    }
	    
	}else{
	    use.tables <- clean.cols
	    names(use.tables) <- sapply(inp.tab.list, "[", 1)
	}
	
	
    }
    
    return(join(.data, use.tables))
}

get.tables.from.vars <- function(col.list)
{
    tb.list <- lapply(col.list, function(x)
	   {
		split.x <- strsplit(x, "\\.")[[1]]
	    
		if (length(split.x) == 1)
		{
		    return(NULL)
		    
		}else if(length(split.x) == 2){
		    
		    return(split.x[1])
		    
		}else{
		    stop("ERROR: Unexpected format of supplied variable")
		}
	   })
    
    return(tb.list)
}

setGeneric("populate", def=function(obj, ...) standardGeneric("populate"))
setMethod("populate", signature("Database"), function(obj, ins.vals=NULL, use.tables=NULL, should.debug=FALSE)
	  {
	    db.con <- dbConnect(SQLite(), dbFile(obj))
	    
	    .populate(schema(obj), db.con, ins.vals=ins.vals, use.tables=use.tables, should.debug=should.debug)
	    
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
}

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
#setGeneric("foreignExtKeyCols", def=function(obj, ...) standardGeneric("foreignExtKeyCols"))
#setMethod("foreignExtKeyCols", signature("TableSchemaList"), function(obj, table.name)
#          {
#            as.character(sapply(return.element(obj, "foreign.keys")[table.name], function(x)
#                   {
#                        as.character(unlist(sapply(names(x), function(y)
#                               {
#                                    return(x[[y]]$ext.keys)
#                               })))
#                   }))
#          })

setGeneric("foreignExtKeyCols", def=function(obj, ...) standardGeneric("foreignExtKeyCols"))
setMethod("foreignExtKeyCols", signature("TableSchemaList"), function(obj, table.name)
          {
	    lapply(return.element(obj, "foreign.keys")[[table.name]], function(x)
		   {
			return(x$ext.keys)
		   })
          })

#setGeneric("foreignLocalKeyCols", def=function(obj, ...) standardGeneric("foreignLocalKeyCols"))
#setMethod("foreignLocalKeyCols", signature("TableSchemaList"), function(obj, table.name, join.tables=NULL)
#          {
#            as.character(sapply(return.element(obj, "foreign.keys")[table.name], function(x)
#                   {
#			if (is.null(join.tables))
#			{
#			    as.character(unlist(sapply(names(x), function(y)
#                               {
#                                    return(x[[y]]$local.keys)
#                               })))
#			}
#			else
#			{
#			    as.character(unlist(sapply(join.tables, function(y)
#                               {
#                                    return(x[[y]]$local.keys)
#                               })))
#			}
#                        
#                   }))
#          })

setGeneric("foreignLocalKeyCols", def=function(obj, ...) standardGeneric("foreignLocalKeyCols"))
setMethod("foreignLocalKeyCols", signature("TableSchemaList"), function(obj, table.name, join.table=NULL)
          {
	    ret.list <- lapply(return.element(obj, "foreign.keys")[[table.name]], function(x)
		   {
			return(x$local.keys)
		   })
	    
	    if (is.null(join.table))
	    {
		return(ret.list)
	    }else if (length(join.table) == 1){
		return(ret.list[[join.table]])
	    }else{
		return(ret.list[join.table])
	    }
            
          })

setGeneric("foreignExtKeySchema", def=function(obj, ...) standardGeneric("foreignExtKeySchema"))
setMethod("foreignExtKeySchema", signature("TableSchemaList"), function(obj, table.name)
          {
	    
	    ext.keys <- foreignExtKeyCols(obj, table.name)
	    
	    ret.list <- lapply(names(ext.keys), function(x,e.k)
		   {
			colSchema(obj, x, mode="normal")[match(e.k[[x]], colNames(obj, x, mode="normal"))]
		   }, ext.keys)
	    
	    names(ret.list) <- names(ext.keys)
	    return(ret.list)
	    
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
            
	    #this is a fix on 8-18-2014 to allow a relationship between two tables to be specified using the same column(s)
	    
	    return(any(sapply(return.element(sub.obj, "foreign.keys"), function(x)
		   {
			if (is.null(x))
			{
			    return(F)
			}
			else if (length(intersect(x$local.keys, x$ext.keys)) == length(union(x$local.keys, x$ext.keys)))
			{
			    return(F)
			}
			else
			{
			    return(T)
			}
		   })))
	    
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

#need to make colNames and colSchema consistent and deal with the direct keys
setGeneric("colNames", def=function(obj, ...) standardGeneric("colNames"))
setMethod("colNames", signature("TableSchemaList"), function(obj, table.name, mode=c("normal", "merge"))
          {
                .get.names.schema(obj, table.name, mode, type="cols")
          })

setGeneric("colSchema", def=function(obj, ...) standardGeneric("colSchema"))
setMethod("colSchema", signature("TableSchemaList"), function(obj, table.name, mode=c("normal", "merge"))
          {
                .get.names.schema(obj, table.name, mode, type="schema")
          })

.get.names.schema <- function(obj, table.name, mode=c("normal", "merge"), type=c("cols", "schema"))
{
    table.mode <- match.arg(mode)
    type <- match.arg(type)
    sub.obj <- subset(obj, table.name)
    
    base.schema <- as.character(return.element(sub.obj, "db.schema"))
    base.cols <- as.character(return.element(sub.obj, "db.cols"))
    
    if (table.mode == "normal")
    {
	return(switch(type, cols=base.cols, schema=base.schema))
    }
    else
    {
	foreign.schema <- foreignExtKeySchema(obj, table.name)
	foreign.cols <- foreignExtKeyCols(obj, table.name)
	local.cols <- foreignLocalKeyCols(obj, table.name)
	
	direct.keys <- directKeys(obj, table.name)
	
	keep.foreign.cols <- unlist(mapply(function(x,y){
				    #also remove the columns that will be present in the final table but not part of the initial table but keep the direct keys
				    rm.cols <- setdiff(y, direct.keys)
				    should.keep <- (x %in% rm.cols == F) & (x %in% base.cols == F)
				    return(should.keep)
			       }, foreign.cols, local.cols))
	
	rm.base.cols <- base.cols %in% setdiff(unlist(local.cols), direct.keys)
	rm.base.cols <- rm.base.cols | base.schema == "INTEGER PRIMARY KEY AUTOINCREMENT"
	
	if (type == "cols")
	{
	    return(as.character(c(unlist(foreign.cols)[keep.foreign.cols], base.cols[rm.base.cols == F])))
	}
	else
	{
	    return(as.character(c(unlist(foreign.schema)[keep.foreign.cols], base.schema[rm.base.cols == F])))
	}
    }
    
    
}

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

setGeneric("directKeys", def=function(obj, ...) standardGeneric("directKeys"))
setMethod("directKeys", signature("TableSchemaList"), function(obj, table.name)
	  {
		all.keys <- return.element(obj, "foreign.keys")[[table.name]]
		
		is.direct.keys <- sapply(all.keys, function(x) length(intersect(x$local.keys, x$ext.keys)) == length(union(x$local.keys, x$ext.keys)))
		
		return(unique(as.character(unlist(all.keys[is.direct.keys]))))
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
                        obj@tab.list[[to]]$foreign.keys <- append(obj@tab.list[[to]]$foreign.keys, use.fk)
                    }
                    
                    #also modify db.cols and db.schema to reflect the new keys/relationships if a non one-to-one relationship is specified
                    
		    if (length(intersect(cur.rhs, cur.lhs)) != length(union(cur.rhs, cur.lhs)))
		    {
			#if any of the previous keys are one-to-one keys then keep them as part of the table
			
			direct.keys <- directKeys(obj, to)
			
			cur.rhs <- setdiff(cur.rhs, direct.keys)
			
			which.rhs <- which(obj@tab.list[[to]]$db.cols %in% cur.rhs)
			obj@tab.list[[to]]$db.cols <- obj@tab.list[[to]]$db.cols[-which.rhs]
			obj@tab.list[[to]]$db.schema <- obj@tab.list[[to]]$db.schema[-which.rhs] 
			
			obj@tab.list[[to]]$db.cols <- append(obj@tab.list[[to]]$db.cols, cur.lhs)
			
			#add in what the schema is in from, cleaning a little for integer autoincrement
			
			curm <- match(cur.lhs, obj@tab.list[[from]]$db.cols)
			
			obj@tab.list[[to]]$db.schema <- append(obj@tab.list[[to]]$db.schema, sapply(strsplit(obj@tab.list[[from]]$db.schema[curm], "\\s+"), "[[", 1))
		    }
		    
                    validObject(obj)
                    return(obj)
                 })
