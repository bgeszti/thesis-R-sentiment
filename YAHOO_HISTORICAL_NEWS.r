library(XML)
library(RCurl)
library(boilerpipeR)
library(ROracle)
library(chron)
library(openNLP)
library(NLP)
library(datamart)


yahooparseFunc = function(u, ticker, formattedDate) {
    result <- tryCatch({
        # building HTML tree for web page 'u' and parse the components using xpathSApply
        htmlURL = htmlParse(u)
        upperdate = xpathSApply(htmlURL, "//div[@class = \"mod yfi_quote_headline withsky\"]/h3[1]/span", 
            xmlValue)
        day = unlist(strsplit(upperdate, ","))
        day = as.numeric(gsub("[^\\d]+", "", day[2], perl = TRUE))
        if (day == as.POSIXlt(formattedDate)$mday) {
            titles = xpathSApply(htmlURL, "//div[@class = \"mod yfi_quote_headline withsky\"]/ul[2]/li//a", 
                xmlValue)
            links = xpathSApply(htmlURL, "//div[@class = \"mod yfi_quote_headline withsky\"]/ul[2]/li//a/@href")
            links = gsub(".*\\*", "", links)
            result = cbind(as.data.frame(links), titles)
        }
    }, error = function(e) {
        result = NULL
    })
    if (!is.null(result)) {
        articles = linkparseFunc(result[, 1])
        previews = senparseFunc(articles)
        pubdate = as.POSIXct(paste(formattedDate, "00:00:00"), format = "%Y-%m-%d", tz = "GMT")
        
        ids = format(pubdate, "%Y%m%d")
        if (nrow(result) == length(articles)) {
            result = cbind(ids, result, previews, pubdate, articles)
        }
    }
}


senparseFunc = function(articles) {
    sent_token_annotator <- Maxent_Sent_Token_Annotator()
    # extract the first five sentences from the articles
    previews = c()
    for (i in 1:length(articles)) {
        if (articles[i] != "") {
            s <- as.String(articles[i])
            sent <- annotate(s, sent_token_annotator)
            prev <- tryCatch({
                prev = head(s[sent], 5)
            }, error = function(e) {
                prev = s[sent]
            })
            preview = paste(prev, sep = "", collapse = "")
            previews[i] = preview
        } else {
            previews[i] = ""
        }
    }
    previews = gsub("<.*?>", "", previews)
}


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
            # extract content from article
            article = ArticleExtractor(content)
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
    gsub("<.*?>", "", articles)
}

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
dsource = "Yahoo"
tickers = c("AAPL", "BAC", "GE", "AVP", "TEVA", "ORCL", "MS", "KO", "MCD", "PG")
dates = seq(as.Date("2015/12/08"), as.Date("2015/12/18"), "days")

for (day in dates) {
    formattedDate = format(as.Date(day, origin = "1970/01/01"), "%Y-%m-%d")
    for (ticker in tickers) {
        # create links
        u = paste("http://finance.yahoo.com/q/h?s=", ticker, "&t=", formattedDate, sep = "")
        print(u)
        # calling yahooparseFunc to parse required elements from page 'u'
        result = yahooparseFunc(u, ticker, formattedDate)
        if (!is.null(result)) {
            # merge results
            result = cbind(result, ticker, dsource)
            for (i in 1:length(result)) {
                if (i != 1 && i != 5) {
                  # encode results, remove html codes
                  result[, i] = enc2utf8(as.vector(result[, i]))
                  result[, i] = strdehtml(result[, i])
                }
            }
            # batch data upload to the DB
            loadFunc(result)
        }
    }
}


