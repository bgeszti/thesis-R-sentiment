# get quotes from yahoo as csv, then store it in the quotes table
GetData <- function() {
    # establish connection using ROracle
    require("ROracle")
    require("chron")
    drv <- dbDriver("Oracle")
    host <- ""
    port <- 1521
    svc <- ""
    # create connection string
    connect.string <- paste("(DESCRIPTION=", "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", 
        port, "))", "(CONNECT_DATA=(SERVICE_NAME=", svc, ")))", sep = "")
    con <- dbConnect(drv, username = "", password = "", dbname = connect.string)
    # download data in csv format and put in the DB
    
    sAddress <- paste("http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,AAPL,BAC,GE,AVP,TEVA,ORCL,MS,KO,MCD,PG&f=sd1t1l1k3ohgbav", 
        sep = "")
    dfYahooData <- read.csv(sAddress, header = F, stringsAsFactors = FALSE)
    dfYahooData$V3 <- times(format(as.POSIXct(dfYahooData$V3, format = "%I:%M %p"), "%H:%M:%S"))
    dfYahooData$V2 = as.Date(dfYahooData$V2, format = "%m/%d/%Y")
    dfYahooData = within(dfYahooData, V12 <- paste(dfYahooData$V2, dfYahooData$V3))
    dfYahooData = dfYahooData[, c(1, 4:12)]
    dfYahooData$V12 = as.POSIXct(dfYahooData$V12, tz = "UTC", format = "%Y-%m-%d %H:%M:%S")
    dfYahooData$V13 = as.double((format(Sys.Date(), "%Y%m%d")))
    sql = "INSERT into QUOTES (TICKER,LT_PRICE,LT_SIZE,OP,D_HIGH,D_LOW,BID,ASK,VOL,LT_TIME,DAY_ID) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)"
    rs <- dbSendQuery(con, sql, data = dfYahooData)
    dbClearResult(rs)
    dbCommit(con)
    dbDisconnect(con)
}
GetData() 
