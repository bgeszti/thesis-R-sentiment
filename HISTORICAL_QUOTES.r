# gets historical quotes from yahoo, then stores it in the daily_quotes table
GetHistoricalData <- function() {
    # establish database connection using ROracle
    require("ROracle")
    require("chron")

    tickers = c("AAPL", "BAC", "GE", "AVP", "TEVA", "ORCL", "MS", "KO", "MCD", "PG")
    Sys.setenv(TZ = "GMT")

    # download data and put in the DB
    for (ticker in tickers) {
        # get data from 2015-12-08 to 2015-12-17
        sAddress <- paste("http://real-chart.finance.yahoo.com/table.csv?s=", ticker, "&a=11&b=08&c=2015&d=11&e=17&f=2015&g=d&ignore=.csv",
            sep = "")
        dailyQuotes <- read.csv(sAddress, header = T, stringsAsFactors = FALSE)
        dailyQuotes$Date = as.POSIXct(dailyQuotes$Date, format = "%Y-%m-%d")
        dailyQuotes = dailyQuotes[, c(1, 2, 3, 4, 7, 6)]
        dailyQuotes$V8 = ticker
        dailyQuotes$V9 = format(strptime(dailyQuotes$Date, format = "%Y-%m-%d"), "%Y%m%d")
        sql = "INSERT into DAILY_QUOTES (DATUM,OP,HI,LO,CL,VO,TICKER,DAY_ID) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)"
        drv <- dbDriver("Oracle")
        host <- ""
        port <- 1521
        svc <- ""
        # create connection string
        connect.string <- paste("(DESCRIPTION=", "(ADDRESS=(PROTOCOL=tcp)(HOST=", host, ")(PORT=",
            port, "))", "(CONNECT_DATA=(SERVICE_NAME=", svc, ")))", sep = "")
        con <- dbConnect(drv, username = "", password = "", dbname = connect.string)
        rs <- dbSendQuery(con, sql, data = dailyQuotes)
        dbClearResult(rs)
        dbCommit(con)
        dbDisconnect(con)
    }
}
GetHistoricalData()
