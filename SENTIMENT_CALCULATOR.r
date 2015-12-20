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

    # Bing Liu scores for fulltext
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

setConnection = function() {
    drv = dbDriver("Oracle")
    host = ""
    port = 1521
    svc = ""
    connect.string = paste("(DESCRIPTION=", "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", 
        port, "))", "(CONNECT_DATA=(SERVICE_NAME=", svc, ")))", sep = "")
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
    # check if there is at least one news for the given day
    ids = dbGetQuery(con, "SELECT DISTINCT DAY_ID FROM COMPANY_NEWS WHERE DAY_ID='20151218' AND TICKER='AAPL' ORDER BY DAY_ID")
    for (i in 1:nrow(ids)) {
        dateid = ids[i, ]
        print(dateid)
        
        # filtering unique news by source_url
        dat_sql = paste("SELECT ID,PREVIEW,FULLTEXT,SOURCE_URL FROM COMPANY_NEWS where ID IN (SELECT MIN(ID) FROM COMPANY_NEWS\n        where DAY_ID='", 
            dateid, "' AND TICKER='AAPL' GROUP BY SOURCE_URL)", sep = "")
        dat = na.omit(dbGetQuery(con, dat_sql))
        result = sentCalc(dat)
        # save the calculated results
        result = cbind(result, dateid)
        load_sql = "INSERT into NEWS_SENTIMENTS (NEWS_ID,GFS,GPS,BFS,BPS,GFPW,GFNW,GPPW,GPNW,BFPW,BFNW,BPPW,BPNW,DAY_ID) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)"
        rs = dbSendQuery(con, load_sql, data = result)
        dbClearResult(rs)
        # commit results
        dbCommit(con)
        print("Data are loaded")
        # closing the connection
    }
    dbDisconnect(con)
}
sentMain() 
