#' @import RMariaDB
#' @import DBI
#' @importFrom yaml read_yaml
#' @importFrom httr cache_info HEAD
#' @import dbplyr
#' @import tidyr
#' @import plyr match_df
#' @import dplyr
#'

REPORT_DIR <- "/home/shepherd/Projects/ReportDatabase/BuildReportDatabase/TempCopyOfFiles/"

update_build_database <- function(dbname,
                                  versions=.get_current_bioc_versions(),
                                  repos=c("bioc", "data-experiment","workflow", "data-annnotation")){

    ## TODO:  add stop if cannot access master.bioconductor.org

    stopifnot(is.character(dbname), length(dbname)==1)
    stopifnot(is.character(versions), all(versions %in% .get_valid_versions()))
    stopifnot(is.character(repos),
              all(repos %in% c("bioc", "data-experiment","workflow", "data-annnotation")))
   
    con <- .connect_to_database(dbname)
    
    for(ver in versions){
        
        message("working on release: ", ver)
        
        ##
        ## TODO: test with multiple repo types
        ##
        # for(repo in repos) {
        repo="bioc"
        message("working on repo: ", repo)

        file <- paste0(
            "https://master.bioconductor.org/checkResults/",
            ver, "/",
            repo, "-LATEST/STATUS_DB.txt"
        )
        date_report <- cache_info(HEAD(file))[["modified"]]


        ## Update Report Table
        
        if(.needs_database_update(con, date_report)){    
            message("Duplicate Report. Not Adding Data.")
            next
        }else{
            reports <- .get_reports_table(con, date_report, repo)
        }

        ## Get Status Table Info Part 1
        status <- .parse_STATUSDB_file(file, ver)
        
        ## Get Builder Table Info
        builder <- .get_builder_table(con, ver)

        ## Update status table with linking ids
        status$builder_id= unname(builder[match(status$builder, rownames(builder)), "builder_id"])
        status$report_id = unname(reports$report_id)

        status <- select(status, c("builder_id", "report_id", "package",
                                   "git_commit_id", "git_commit_date", "stage", "status")) 
        dbAppendTable(con, "status", status)

        
        ## TODO: need to check skipped file for errors to add to status
        
    } ## loop over versions

    ## Disconnect from the database
    dbDisconnect(con)
}


.get_current_bioc_versions <-function(){
    config =  yaml::read_yaml("https://master.bioconductor.org/config.yaml")
    versions <- c(config$release_version, config$devel_version)
}
.get_valid_versions <- function(){
    config =  yaml::read_yaml("https://master.bioconductor.org/config.yaml")
    versions <- c(names(config$release_date), config$devel_version)
}


.connect_to_database <- function(dbname){
    con <- dbConnect(RMariaDB::MariaDB(), group = dbname)
}

.needs_database_update <- function(con, date_report){
    
    # check if date is already in database
    qry <- paste0("SELECT * FROM reports WHERE date='",date_report,"';")
    res <- dbSendQuery(con, qry)
    tbl_reports<- dbFetch(res)
    dbClearResult(res)
    nrow(tbl_reports) != 0    
}

.get_reports_table <- function(con, date_report, repo){

    repo_type <- ifelse(repo == "bioc","software", repo)
    dbAppendTable(con, "reports", data.frame(date=date_report, repo_type=repo_type))
    qry <- paste0("SELECT * FROM reports WHERE date='",date_report,"';")
    res <- dbSendQuery(con, qry)
    tbl_reports<- dbFetch(res)
    dbClearResult(res)
    tbl_reports
}


.parse_STATUSDB_file <- function(file, ver){

    tbl <- read.table(file, comment.char="")
    names(tbl) = c("builder", "status")
    status <- tbl %>% separate(builder, c("package", "builder", "stage"), "#")
    status$stage <- gsub(status$stage, pattern=":", replacement="")
    status$status[which(is.na(status$status))] = "NA"


    gitcommitid <- rep("", dim(status)[1])
    gitcommitdate <- rep("", dim(status)[1])
         
    for(i in seq_len(dim(status)[1])){

        pkg <- status[i, "package"]
        dcf <-
            read.dcf(paste0(REPORT_DIR, ver, "/bioc/gitlog/git-log-", pkg,".dcf"))
        gitcommitid[i] <- dcf[,"Last Commit"]
        gitcommitdate[i] <- dcf[,"Last Changed Date"]
    }
    status <- cbind(status, git_commit_id=gitcommitid, git_commit_date=gitcommitdate)
    status
}


.get_builder_table <- function(con, ver){
    
    ActiveBuilders <- system2("ls", args= paste0(REPORT_DIR, ver, "/bioc/nodes"), stdout=TRUE)
    df <- matrix("", nrow=length(ActiveBuilders), ncol=4)
    rownames(df) <- ActiveBuilders
    colnames(df) <- c("r_version", "platform", "os", "bioc_version")
         
    for(i in ActiveBuilders){
        text <-
            readLines(paste0(REPORT_DIR, ver, "/bioc/nodes/",i,"/NodeInfo/R-sessionInfo.txt"),
                      n=3)
        df[i,] <-  c(trimws(gsub(pattern="Platform:|Running under:", replacement="", text)), ver)
        
    }
    
    res <- dbSendQuery(con, "SELECT * FROM builders")
    builders <- dbFetch(res)
    dbClearResult(res)
    
   ## verify there is an entry in the database and get builder_id for df 
    builder_id <- rep(NA_integer_, nrow(df))
    found <- match_df(builders, as.data.frame(df))
    builder_id[match(unname(unlist(found["builder"])),  rownames(df))] = found$builder_id
         

    ## Update builders table if needed
    if (nrow(df) != nrow(found)){
        
        if(nrow(found) == 0){
            not_fnd <- cbind(as.data.frame(df), builder=rownames(df))
        }else{              
            not_fnd <- df[-(match(found$builder, rownames(df))),,drop=FALSE]
            not_fnd <- cbind(not_fnd, builder=rownames(not_fnd))
            not_fnd <- as.data.frame(not_fnd) %>% select(colnames(builders)[-1])
        }
        dbAppendTable(con, "builders", not_fnd)    
        
        res <- dbSendQuery(con, "SELECT * FROM builders")
        builders <- dbFetch(res)
        dbClearResult(res)
        
        builder_id <- rep(NA_integer_, nrow(df))
        found <- match_df(builders, as.data.frame(df))
        builder_id[match(unname(unlist(found["builder"])),  rownames(df))] = found$builder_id
        
    } else {
        message("All builders found")
    }
    
    df <- cbind(builder_id, df)
    df
}
