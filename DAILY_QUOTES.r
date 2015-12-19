library(quantmod)
library(ROracle)
setConnection = function() {
    drv = dbDriver("Oracle")
    host = ""
    port = 1521
    svc = ""
    connect.string = paste("(DESCRIPTION=", "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=", 
        port, "))", "(CONNECT_DATA=(SERVICE_NAME=", svc, ")))", sep = "")
    con = dbConnect(drv, username = "", password = "", dbname = connect.string)
}
con = setConnection()
stockdf = data.frame()
stocks = c("AAPL", "BAC", "GE", "AVP", "TEVA", "ORCL", "MS", "KO", "MCD", "PG")
for (s in stocks) {
    quotes = getQuote(s, auto.assign = TRUE, return.class = "data.frame", what = yahooQuote.EOD, 
        env = NULL)
    stockdf = rbind(stockdf, quotes)
}
stockdf$date = as.POSIXct(strptime(stockdf$Trade, format = "%Y-%m-%d"), "%Y-%m-%d", tz = "GMT")

stockdf$id = format(stockdf$date, "%Y%m%d")
stockdf$ticker = rownames(stockdf)
dailyQuotes = stockdf[, c(7, 2, 3, 4, 5, 6, 9, 8)]
sql = "INSERT into DAILY_QUOTES (DATUM,OP,HI,LO,CL,VO,TICKER,DAY_ID) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)"
rs <- dbSendQuery(con, sql, data = dailyQuotes)
dbClearResult(rs)
dbCommit(con)
dbDisconnect(con) 
