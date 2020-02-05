#' @include Connection.R
NULL

duckdb_result <- function(connection, stmt_ref, statement, has_resultset) {
  env <- new.env(parent=emptyenv())
  env$rows_fetched <- 0
  env$open <- TRUE

  new("duckdb_result", connection = connection,  statement = statement, stmt_ref=stmt_ref, has_resultset=has_resultset,env=env)
}

#' @rdname DBI
#' @export
setClass(
  "duckdb_result",
  contains = "DBIResult",
  slots = list(
    connection = "duckdb_connection",
    stmt_ref = "externalptr",
    statement = "character",
    has_resultset = "logical",
    resultset = "data.frame",
    rows_affected = "numeric",
    env = "environment"
  )
)



#' @rdname DBI
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "duckdb_result",
  function(object) {
    cat(sprintf("<duckdb_result %s statement='%s' connection=%s>\n", extptr_str(object@stmt_ref), object@statement, extptr_str(object@connection@conn_ref)))
  })

#' @rdname DBI
#' @inheritParams DBI::dbClearResult
#' @export
setMethod(
  "dbClearResult", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
      warning("Result was cleared already")
    }
    res@env$open <- FALSE
    return(invisible(TRUE))
  })

# as per is.integer documentation
is_wholenumber <- function(x, tol = .Machine$double.eps^0.5)  abs(x - round(x)) < tol

fix_rownames <- function(df) {
  attr(df, "row.names") <- c(NA, as.integer(-nrow(df)))
  return(df)
}


#' @rdname DBI
#' @inheritParams DBI::dbFetch
#' @importFrom utils head
#' @export
setMethod(
  "dbFetch", "duckdb_result",
  function(res, n = -1, ...) {
    if (!res@env$open) {
      stop("result set was closed")
    }
    if (length(n) != 1) {
      stop("need exactly one value in n")
    }
    if (is.infinite(n)) {
      n <- -1
    }
    if (n < -1) {
      stop("cannot fetch negative n other than -1")
    }
    if (!is_wholenumber(n)) {
      stop("n needs to be not a whole number")
    }
    if (!res@has_resultset) {
      warning("Cannot fetch from statement result")
      return(data.frame())
    }

    res@env$resultset <- .Call(duckdb_execute_fetch_R, res@stmt_ref)
    print(res@env$resultset)


# FIXME this is ugly
    if (n == 0) {
      return(utils::head(res@resultset, 0))
    }
    if (res@env$rows_fetched < 0) {
      res@env$rows_fetched <- 0
    }
    if (res@env$rows_fetched >= nrow(res@resultset)) {
      return(fix_rownames(res@resultset[F,, drop=F]))
    }
    # special case, return everything
    if (n == -1 && res@env$rows_fetched == 0) {
      res@env$rows_fetched <- nrow(res@resultset)
      return(res@resultset)
    }
    if (n > -1) {
      n <- min(n, nrow(res@resultset) - res@env$rows_fetched)
      res@env$rows_fetched <- res@env$rows_fetched + n
      df <- res@resultset[(res@env$rows_fetched - n + 1):(res@env$rows_fetched),, drop=F]
      return(fix_rownames(df))
    }
    start <- res@env$rows_fetched + 1
    res@env$rows_fetched <- nrow(res@resultset)
    df <- res@resultset[nrow(res@resultset),, drop=F]
    return(fix_rownames(df))
  })

#' @rdname DBI
#' @inheritParams DBI::dbHasCompleted
#' @export
setMethod(
  "dbHasCompleted", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
     stop("result has already been cleared")
    }
    if (!res@has_resultset) {
      return(TRUE)
    }
    return(res@env$rows_fetched == nrow(res@resultset))
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "duckdb_result",
  function(dbObj, ...) {
    # Optional
    getMethod("dbGetInfo", "DBIResult", asNamespace("DBI"))(dbObj, ...)
  })

#' @rdname DBI
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "duckdb_result",
  function(dbObj, ...) {
    return(dbObj@env$open)
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetStatement
#' @export
setMethod(
  "dbGetStatement", "duckdb_result",
  function(res, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }
    return(res@statement)
  })

#' @rdname DBI
#' @inheritParams DBI::dbColumnInfo
#' @export
setMethod(
  "dbColumnInfo", "duckdb_result",
  function(res, ...) {
      return(data.frame(name=names(res@resultset), type=vapply(res@resultset, class, "character"), stringsAsFactors=F))
   return(list())
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetRowCount
#' @export
setMethod(
  "dbGetRowCount", "duckdb_result",
  function(res, ...) {
     if (!res@env$open) {
      stop("result has already been cleared")
    }
   return(res@env$rows_fetched)
  })

#' @rdname DBI
#' @inheritParams DBI::dbGetRowsAffected
#' @export
setMethod(
  "dbGetRowsAffected", "duckdb_result",
  function(res, ...) {
     if (!res@env$open) {
      stop("result has already been cleared")
    }
    return(invisible(res@rows_affected))
  })

#' @rdname DBI
#' @inheritParams DBI::dbBind
#' @importFrom testthat skip
#' @export
setMethod(
  "dbBind", "duckdb_result",
  function(res, params, ...) {
    if (!res@env$open) {
      stop("result has already been cleared")
    }
    testthat::skip("Not yet implemented: dbBind(Result)")

  })
