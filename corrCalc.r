Sys.setenv(TZ = "GMT")
library(ROracle)
library(xts)

# Establish the connection
setConnection = function()
{
    drv = dbDriver("Oracle")
    host = ""
    port = 1521
    svc = ""
    connect.string = paste("(DESCRIPTION=", "(ADDRESS=(PROTOCOL=tcp)(HOST=", 
        host, ")(PORT=", port, "))", "(CONNECT_DATA=(SERVICE_NAME=", svc, 
        ")))", sep = "")
    con = dbConnect(drv, username = "", password = "", dbname = connect.string)
}
con = setConnection()
# Loading the quantmod package to handle xts objects and charting
library(quantmod)
# Downloading stock price data
dat_sql = "SELECT DISTINCT DATUM,OP,CL,VO FROM DAILY_QUOTES where TICKER='AAPL' 
AND DAY_ID BETWEEN '20150924' AND '20151204' 
ORDER BY DATUM"
stocks = na.omit(dbGetQuery(con, dat_sql))
# Create an xts time series object from the dataframe
stocks.xts = xts(stocks[, -1], order.by = stocks$DATUM)
# Calculate daily logarithmic return
stocks.xts$return = diff(log(stocks.xts$CL), lag = 1)
# Calculate daily logarithmic Open to Close return
stocks.xts$OPtoCLreturn = log(stocks.xts$CL) - log(stocks.xts$OP)
# Remove first row (return=NA for the first day...)
stocks.xts = stocks.xts[-1, ]

# Function to calculate the daily sentiment scores
sentCalc = function(s)
{
    sent = sum(s > 0.5)/(sum(s != 0))
}

# Downloading sentiment data
sent_sql = "SELECT DISTINCT s.DAY_ID,s.GFS,s.BFS,to_char(c.RELEASE_TIME, 'HH24:MI:SS') as time FROM SENTIMENTS s
INNER JOIN WEEKDAYS w ON s.DAY_ID=w.ID
INNER JOIN COMPANY_NEWS c ON s.NEWS_ID=c.ID
where s.DAY_ID BETWEEN '20150928' AND '20151204' AND w.ISHOLIDAY=0 
AND c.DATASOURCE='Google'
and to_char(c.RELEASE_TIME, 'HH24:MI:SS') between '07:30:00' and '16:30:00'"

# Aggregate the sentiment scores by day using the sentCalc function
scores = na.omit(dbGetQuery(con, sent_sql))

# Calculate aggregated daily sentiment score
gfs = scores[, c(1, 2)]
bfs = scores[, c(1, 3)]

sent1 = aggregate(cbind(GFS) ~ DAY_ID, data = gfs, FUN = function(gfs) sentCalc(gfs))
sent2 = aggregate(cbind(BFS) ~ DAY_ID, data = bfs, FUN = function(bfs) sentCalc(bfs))
dfs = list(sent1, sent2)
library(plyr)
sent = join_all(list(sent1, sent2), by = "DAY_ID", type = "left")
sent$DAY_ID = strptime(sent$DAY_ID, format = "%Y%m%d")
sent.xts = xts(sent[, -1], order.by = sent$DAY_ID)

weights = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1)
# Calculate correlations
correl = data.frame()
numvals = c("return", "OPtoCLreturn", "CL", "VO")
for (j in 1:length(weights))
{
    
    for (i in 1:length(numvals))
    {
        sent.xts$score = sent.xts$GFS
        merged = na.omit(merge(stocks.xts[, numvals[i]], sent.xts$score))
        c1 = cor(merged[, 1], merged[, 2])
        sent.xts$score = sent.xts$BFS
        merged = na.omit(merge(stocks.xts[, numvals[i]], sent.xts$score))
        c2 = cor(merged[, 1], merged[, 2])
        sent.xts$score = ((1 - weights[j]) * sent.xts$GFS + weights[j] * 
            sent.xts$BFS)/2
        merged = na.omit(merge(stocks.xts[, numvals[i]], sent.xts$score))
        c3 = cor(merged[, 1], merged[, 2])
        res = c(c1, c2, c3, weights[j], (1 - weights[j]))
        correl = rbind(correl, res)
    }
}
colnames(correl) = c("GI Score", "Bing Liu Score", "Combined Score", "GI weight", 
    "Bing weight")
rownames(correl) = c("Return", "OPtoCLreturn", "Close", "Volume")
# Display the results
correl
mcorr = as.matrix(correl)
library(corrplot)
corrplot(mcorr, method = "color")
library(xtable)
print(xtable(mcorr), type = "html")

# Charting
sent.xts$score = sent.xts$GFS
sent.xts$score = (sent.xts$BFS + sent.xts$GFS)/2
merged = na.omit(merge(stocks.xts$return, sent.xts$score))
# chartSeries(merged[,1],theme='white',name='Daily returns of
# Apple',TA='addSMA(n = 3, overlay = TRUE, col = 'red')')
chartSeries(merged[, 1], theme = "white", name = "Daily returns of Apple")
x11()
chartSeries(merged[, 2], theme = "white", name = "Sentiment scores of Apple news")
# chartSeries(merged[,2], theme='white',name='Sentiment
# scores',TA='addSMA(n = 3, overlay = TRUE, col = 'blue')')


# Calculate cross correlation
Return = as.vector(merged[, 1])
sentScore = as.vector(merged[, 2])
ccf(Return, sentScore, lag.max = 3)

# Calculate Granger Causality
library(lmtest)
Return <- na.omit(diff(merged[,1]))
Score <- na.omit(diff(merged[,2]))
grangertest(Return ~ Score, order=1)


