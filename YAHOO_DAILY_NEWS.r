library(XML)
library(RCurl)
library(boilerpipeR)
library(ROracle)
library(chron)
library(openNLP)
library(NLP)
library(datamart)

sentCalc = function(dat) {
    
    
    ToSentences = function(txt, language = "en") {
        # split text at sentence termination marks of the given language
        if (nchar(txt) == 0) {
            return("")
        }
        txt = as.String(txt)
        sent_token_annotator = Maxent_Sent_Token_Annotator(language = language)
        # Return sentences by splitting the text at the markers
        markers = NLP::annotate(txt, Maxent_Sent_Token_Annotator(language = language)  # Annotator from OpenNLP
)
        txt[markers]
    }
    
    CorpusToSentences = function(corpus) {
        # Split every document in the corpus into sentences and return a new corpus with
        # all the sentences as individual documents.  Extract the text from each document
        # in the corpus.
        tx = lapply(corpus, "[[", "content")
        # Convert the text
        sentences = lapply(tx, ToSentences)
        sentences = sapply(sentences, tolower)
        filter = "aapl|apple"
        # Filter out relevant sentences
        filteredSentences = sentences[grepl(filter, sentences)]
        # Return a corpus with sentences as documents.
        corpus = Corpus(VectorSource(filteredSentences))
    }
    
    # Preprocess documents, score them using the Harvard GI (dictionary = 1) and Bing Liu (dictionary = 0)
    # dictionaries: pos:1, neg:-1, neutral:0
    scoreCalc = function(dat, id, dictionary) {
        # Loading the Bing Liu lexicon
        neg.terms = read.csv("negative_words.csv", stringsAsFactors = FALSE)
        pos.terms = read.csv("positive_words.csv", stringsAsFactors = FALSE)
        
        scoresdf = data.frame(id = NULL, score = NULL)
        posdf = data.frame(id = "", words = "", stringsAsFactors = FALSE)
        negdf = data.frame(id = "", words = "", stringsAsFactors = FALSE)
        for (i in 1:length(dat)) {
            newsid = id[i]
            corp = Corpus(VectorSource(dat[i]))
            corpus = CorpusToSentences(corp)
            if (summary(corpus)[1] != 0) {
                # remove all uneccessary content like special characters, stopwords and apply stemming
                corpus = tm_map(corpus, removePunctuation, preserve_intra_word_dashes = TRUE)
                corpus = tm_map(corpus, removeWords, stopwords("english"))
                corpus = tm_map(corpus, removeNumbers)
                corpus = tm_map(corpus, stemDocument, language = "english")
                corpus = tm_map(corpus, stripWhitespace)
                corpus = tm_map(corpus, PlainTextDocument)
                
                
                dtm = DocumentTermMatrix(corpus)
                dtm = dtm[, colSums(as.matrix(dtm)) > 0]
                terms = colnames(dtm)
                
                # Find the positive and negative terms using the lexicons.
                if (dictionary) {
                  pos.words = terms[terms %in% terms_in_General_Inquirer_categories("Positiv")]
                  neg.words = terms[terms %in% terms_in_General_Inquirer_categories("Negativ")]
                } else {
                  pos.words = terms[terms %in% pos.terms[, 1]]
                  neg.words = terms[terms %in% neg.terms[, 1]]
                }
                # Calculate the negative and positive sentence scores ('document scores').
                neg.score = rowSums(as.matrix(dtm[, neg.words]))
                pos.score = rowSums(as.matrix(dtm[, pos.words]))
                
                document.score = pos.score - neg.score
                # Calulate the sentences signs.
                document.sign = sign(document.score)
                
                sentiment.score = sum(document.sign == 1)/sum(document.sign != 0)
                if (is.nan(sentiment.score)) {
                  sentiment.score = 0
                }
            } else { # handling empty documents
                sentiment.score = 0
                pos.words = NULL
                neg.words = NULL
                print(newsid)
                
            }
            scoresdf = rbind(scoresdf, c(newsid, sentiment.score))
            pos.words = as.character(paste(pos.words, collapse = " "))
            neg.words = as.character(paste(neg.words, collapse = " "))
            posdf = rbind(posdf, c(newsid, pos.words))
            negdf = rbind(negdf, c(newsid, neg.words))
        }
        
        out = list(scoresdf, posdf, negdf)
        return(out)
        
    }
    
    # Harvard GI scores for fulltext
    out = scoreCalc(dat$FULLTEXT, dat$ID, 1)
    df1 = out[[1]]
    gfp = out[[2]]
    gfn = out[[3]]

    # Harvard GI scores for preview
    out = scoreCalc(dat$PREVIEW, dat$ID, 1)
    df2 = out[[1]]
    gpp = out[[2]]
    gpn = out[[3]]

    # Bing Liu scores for fulltext
    out = scoreCalc(dat$FULLTEXT, dat$ID, 0)
    df3 = out[[1]]
    bfp = out[[2]]
    bfn = out[[3]]

    # Bing Liu scores for preview
    out = scoreCalc(dat$PREVIEW, dat$ID, 0)
    df4 = out[[1]]
    bpp = out[[2]]
    bpn = out[[3]]
    
    colnames = c("id", "score")
    dfs = list(df1, df2, df3, df4)
    for (i in 1:length(dfs)) {
        colnames(dfs[[i]]) = colnames
    }
    # join all scores for insertion into NEWS_SENTIMENTS table
    merged = join_all(list(dfs[[1]], dfs[[2]], dfs[[3]], dfs[[4]], gfp, gfn, gpp, 
        gpn, bfp, bfn, bpp, bpn), by = "id", type = "left")
    return(merged)
}

setConnection = function() {
    # connect to the database server
    drv = dbDriver("Oracle")
    host = ""
    port = 1521
    svc = ""
    connect.string = paste("(DESCRIPTION=", "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, 
        ")(PORT=", port, "))", "(CONNECT_DATA=(SERVICE_NAME=", svc, ")))", sep = "")
    con = dbConnect(drv, username = "", password = "", dbname = connect.string)
}


sentMain = function() {
    library(XML)
    library(RCurl)
    library(boilerpipeR)
    library(ROracle)
    library(chron)
    library(datamart)
    library(tm)
    library(tm.plugin.webmining)
    library(edgar)
    library(NLP)
    library(openNLP)
    require(tm.lexicon.GeneralInquirer)
    require(edgar)
    require(plyr)
    con = setConnection()
    dateid = format(Sys.Date(), "%Y%m%d")
    # get all news from today which aren't scored yet
    dat_sql = paste("SELECT ID,PREVIEW,FULLTEXT,SOURCE_URL FROM COMPANY_NEWS  \n\tWHERE DAY_ID='", 
        dateid, "'AND COMPANY_NEWS.ID NOT IN \n\t(SELECT NEWS_ID FROM NEWS_SENTIMENTS WHERE DAY_ID='", 
        dateid, "')", sep = "")
    
    dat = na.omit(dbGetQuery(con, dat_sql))
    if (!(nrow(dat) == 0)) {
        result = sentCalc(dat)
        result = cbind(result, dateid)
        load_sql = "INSERT into NEWS_SENTIMENTS (NEWS_ID,GFS,GPS,BFS,BPS,GFPW,GFNW,GPPW,GPNW,BFPW,BFNW,BPPW,BPNW,DAY_ID) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)"
        rs = dbSendQuery(con, load_sql, data = result)
        dbClearResult(rs)
        # commint results
        dbCommit(con)
        print("Data are loaded")
        dbDisconnect(con)
        # closing the connection
    }
}

yahooparseFunc = function(u, ticker, formattedDate) {
    # parse the yahoo webpage, extracting the relevant data using xpath queries
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
            d = xpathSApply(htmlURL, "//div[@class = \"mod yfi_quote_headline withsky\"]/ul[2]/li//cite/span", 
                xmlValue)
            datum = c()
            for (i in 1:length(d)) {
                datum[i] = unlist(strsplit(d[i], " "))[2]
            }
            pubdate = strptime(paste(formattedDate, datum), format = "%Y-%m-%d %I:%M%p", 
                tz = "GMT")
            result = cbind(as.data.frame(links), titles)
        }
    }, error = function(e) {
        result = NULL
    })
    if (!is.null(result)) {
        articles = linkparseFunc(result[, 1])
        previews = senparseFunc(articles)
        # pubdate=as.POSIXct(paste(formattedDate,'00:00:00'),format='%Y-%m-%d',tz='GMT')
        
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
    # cleans the link, downloads the article and extract the main content from the web page as plain text
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

loadFunc = function(result) {
    # load the data into database
    con = setConnection()
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

updateFilter = function(result, ticker, day) {
    con = setConnection()
    dayid = format(day, "%Y%m%d")
    sql = paste("SELECT SOURCE_URL FROM YAHOO_NEWS_TEST WHERE DAY_ID='", dayid, "' AND TICKER='", 
        ticker, "' AND DATASOURCE='Yahoo'", sep = "")
    rs = dbGetQuery(con, sql)
    res = result[!(result[, 2] %in% rs[, 1]), ]
    res
}

Sys.setenv(TZ = "GMT")
dsource = "Yahoo"
tickers = c("AAPL", "BAC", "GE", "AVP", "TEVA", "ORCL", "MS", "KO", "MCD", "PG")
day = as.Date(Sys.Date())

for (ticker in tickers) {
    # get finance news links from yahoo in standard HTML format
    u = paste("http://finance.yahoo.com/q/h?s=", ticker, "&t=", day, sep = "")
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
        result = updateFilter(result, ticker, day)
        if (!(nrow(result) == 0)) {
            loadFunc(result)
            sentMain()
        }
    }
}

