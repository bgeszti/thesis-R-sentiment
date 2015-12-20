library(XML)
library(RCurl)
library(boilerpipeR)
library(ROracle)
library(chron)
library(XML)
library(RCurl)
library(datamart)



# building HTML tree for web page 'u' and parse the components using xpathSApply
parseFunc = function(u, ticker) {
    
    result <- tryCatch({
        htmlURL = htmlParse(u)
        # parse the RSS tags with handling some invalid markup
        links = xpathSApply(htmlURL, "//item/link/following-sibling::text()", xmlValue)
        titles = xpathSApply(htmlURL, "//item/title", xmlValue)
        previews = xpathSApply(htmlURL, "//item/description", xmlValue)
        pubDates = xpathSApply(htmlURL, "//item/*[4]", xmlValue)
        result = cbind(links, titles, previews, pubDates)
    }, error = function(e) {
        result = NULL
    })
    
    # filter out unrelevant items (items where the title doesn't contain the ticker or the
    # name of the company)
    if (!(is.null(result))) {
        fResult = as.data.frame(result[filterFunc(result, ticker), ])
        if (ncol(fResult) == 1) {
            fResult = as.data.frame(t(fResult))
        }
        # removing html tags from the descriptions
        fResult[, 3] = gsub("<.*?>", "", fResult[, 3])
        # converting GTM time to EST
        fResult[, 4] = as.POSIXct(gsub(" GMT", "", fResult[, 4]), format = "%a, %d %b %Y %H:%M:%S")
        fResult[, 4] = fResult[, 4] - as.difftime(5, unit = "hours")
        ids = format(fResult[, 4], "%Y%m%d")
        # parsing articles
        articles = linkparseFunc(fResult[, 1])
        if (nrow(fResult) == length(articles)) {
            result = cbind(ids, fResult, articles)
        }
    }
}

filterFunc = function(result, ticker) {
    toMatch = data.frame(AAPL = "AAPL|Apple", BAC = "BAC|Bank of America", GE = "GE|General Electric", 
        AVP = "AVP|Avon", TEVA = "TEVA|Teva", ORCL = "ORCL|Oracle", MS = "MS|Morgan", KO = "KO|Coca", 
        MCD = "MCD|McDonald", PG = "PG|Procter")
    pattern = levels(toMatch[, ticker])
    rowIndices = grep(pattern, result[, 2])
}

# cleans the link, downloads the article and extract the main content from the web page as plain text
linkparseFunc = function(link) {
    articles = c()
    links = gsub("[\r\n]", "", link)
    
    for (i in 1:length(links)) {
        content = tryCatch({
            # download article URL
            getURL(links[i])
        }, error = function(e) {
            content = NULL
        })
        print(links[i])
        if (!(is.null(content))) {
            # extract content from article with boilerpipeR
            article = ArticleExtractor(content)
            # convert the text encoding to ascii, translating any unicode character to the ascii variant
            article <- iconv(article, "UTF-8", "ASCII//TRANSLIT")
            # if no results given by getURL(), try again once
            if (is.na(article)) {
                content = tryCatch({
                  getURL(links[i])
                }, error = function(e) {
                  content = NULL
                })
                article = ArticleExtractor(content)
                article <- iconv(article, "UTF-8", "ASCII//TRANSLIT")
            }
            if (!is.na(article)) {
                articles[i] = article
            } else {
                articles[i] = ""
            }
        } else {
            articles[i] = ""
        }
    }
    # strip HTML tags
    gsub("<.*?>", "", articles)
}

# load data into the database table
loadFunc = function(result) {
    drv <- dbDriver("Oracle")
    host <- ""
    port <- 1521
    svc <- ""
    connect.string <- paste("(DESCRIPTION=", "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", 
        port, "))", "(CONNECT_DATA=(SERVICE_NAME=", svc, ")))", sep = "")
    # create connection with given driver and connection string
    con <- dbConnect(drv, username = "", password = "", dbname = connect.string)
    # create sql statement
    sql = "INSERT into COMPANY_NEWS (DAY_ID,SOURCE_URL,TITLE,PREVIEW,RELEASE_TIME,FULLTEXT,TICKER,DATASOURCE) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)"
    # batch data load
    rs <- dbSendQuery(con, sql, data = result)
    dbClearResult(rs)
    # commint results
    dbCommit(con)
    print("Data are loaded")
    # closing the connection
    dbDisconnect(con)
}

Sys.setenv(TZ = "GMT")
dsource = "Google"
tickers = c("AAPL", "BAC", "GE", "AVP", "TEVA", "ORCL", "MS", "KO", "MCD", "PG")
dates = seq(as.Date("2015/11/07"), as.Date("2015/11/07"), "days")
for (day in dates) {
    
    formattedDate = format(as.Date(day, origin = "1970/01/01"), "%Y-%m-%d")
    for (ticker in tickers) {
        # get finance news links from google in RSS format
        u = paste("http://www.google.com/finance/company_news?q=", ticker, "&output=rss&startdate=", 
            formattedDate, "&enddate=", formattedDate, "&num=100", sep = "")
        print(u)
        # calling parseFunc to parse required elements from page 'u'
        result = parseFunc(u, ticker)
        if (!is.null(result)) {
            for (i in 1:length(result)) {
                if (i != 1 && i != 5) {
                  # encode results, remove html codes
                  result[, i] = enc2utf8(as.vector(result[, i]))
                  result[, i] = strdehtml(result[, i])
                }
            }
            # merge results
            result = cbind(result, ticker, dsource)
            # batch data upload to the DB
            
            loadFunc(result)
            
        }
    }
}

