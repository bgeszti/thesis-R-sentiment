library(XML)
library(RCurl)
library(boilerpipeR)
library(ROracle)
library(chron)
library(XML)
library(RCurl)
library(datamart)

sentCalc = function(dat) {
    
    
    ToSentences = function(txt, language = "en") {
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
        # Split every document in the corpus into sentences and return a new corpus with all the
        # sentences as individual documents.  Extract the text from each document in the corpus.
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

    # Preprocess documents, score them using the Harvard GI (dictionary = 1) and Bing Liu (dictionary = 0) dictionaries
    # pos:1, neg:-1, neutral:0
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
    merged = join_all(list(dfs[[1]], dfs[[2]], dfs[[3]], dfs[[4]], gfp, gfn, gpp, gpn, bfp,
        bfn, bpp, bpn), by = "id", type = "left")
    return(merged)
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
setConnection = function() {
    drv = dbDriver("Oracle")
    host = ""
    port = 1521
    svc = ""
    connect.string = paste("(DESCRIPTION=", "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", 
        port, "))", "(CONNECT_DATA=(SERVICE_NAME=", svc, ")))", sep = "")
    con = dbConnect(drv, username = "", password = "", dbname = connect.string)
}


loadFunc = function(result) {
    con = setConnection()
    # create connection with given driver and connection string create sql statement
    sql = "INSERT into YAHOO_NEWS_TEST (DAY_ID,SOURCE_URL,TITLE,PREVIEW,RELEASE_TIME,FULLTEXT,TICKER,DATASOURCE) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)"
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
    dayid = format(day - 1, "%Y%m%d")
    sql = paste("SELECT SOURCE_URL FROM YAHOO_NEWS_TEST WHERE DAY_ID='", dayid, "' AND TICKER='", 
        ticker, "' AND DATASOURCE='Google'", sep = "")
    rs = dbGetQuery(con, sql)
    res = result[!(result[, 2] %in% rs[, 1]), ]
    res
}

Sys.setenv(TZ = "GMT")
dsource = "Google"
tickers = c("AAPL", "BAC", "GE", "AVP", "TEVA", "ORCL", "MS", "KO", "MCD", "PG")

day = as.Date(Sys.Date() + 1)

for (ticker in tickers) {
    # get finance news links from google in RSS format
    u = paste("http://www.google.com/finance/company_news?q=", ticker, "&output=rss&startdate=", 
        day, "&enddate=", day, "&num=100", sep = "")
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
        result = updateFilter(result, ticker, day)
        if (!(nrow(result) == 0)) {
            loadFunc(result)
            sentMain()
        }
    }
} 
