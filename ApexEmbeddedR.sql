#creates a Candlechart
BEGIN
SYS.RQSCRIPTDROP('CandleChart');
SYS.RQSCRIPTCREATE('CandleChart',
'function(stocks)
{
library(xts)
library(quantmod)
rownames(stocks)=stocks$DATUM
stocks.xts =xts(stocks[,-1], order.by=stocks$DATUM)
colnames(stocks.xts) <- c("open", "high", "low", "close","volume")
candleChart(stocks.xts,theme="white", major.ticks="days",type="candles",subset="last 10 weeks")}');
END

#creates a Technical chart
BEGIN
SYS.RQSCRIPTDROP('TechnicalChart');
SYS.RQSCRIPTCREATE('TechnicalChart',
'function(stocks)
{
library(xts)
library(quantmod)
require(TTR)
rownames(stocks)=stocks$DATUM
stocks.xts =xts(stocks[,-1], order.by=stocks$DATUM)
colnames(stocks.xts) <- c("open", "high", "low", "close","volume")
chartSeries(stocks.xts,subset="last 8 weeks",type="line",theme="white",
TA="addSMA(n=5);addROC(n=5);addRSI()")}');
END;

#Display daily returns
BEGIN
SYS.RQSCRIPTDROP('RetRep');
SYS.RQSCRIPTCREATE('RetRep',
'
function(stocks)
{
library(plyr)
library(reshape2)
stocks$return <- ave(stocks$CL, factor(stocks$TICKER), 
FUN=function(x) c(-1*(diff(log(x),)),NA))
stocks=na.omit(stocks)
}');
END;

#Create forecast plot
BEGIN
SYS.RQSCRIPTDROP('Forecast');
SYS.RQSCRIPTCREATE('Forecast',
'function(stocks)
{
library(TTR)
ordered <- stocks[order(as.Date(stocks$DATUM, format="%Y-%m-%d")),]
tstocks <- ts(ordered$CL,frequency=5)
ses1 = HoltWinters(tstocks,beta = FALSE, gamma = FALSE)
ses2 = HoltWinters(tstocks, gamma = FALSE)
ses3 = HoltWinters(tstocks)
pred1=predict(ses1,7)
pred2=predict(ses2,7)
pred3=predict(ses3,7)
a=format(seq(as.Date("2015-09-28"), Sys.Date(), "weeks"),"%b-%d");
plot(tstocks,type="l",lwd=0.5,main="Exponential smoothing forecast",xaxt="n")
lines(pred1,col="red",lwd=4)
lines(pred2,col="blue",lwd=4)
lines(pred3,col="green",lwd=4)
axis(1, 1:length(a), labels = a, cex.axis=0.65,las=2)
}');
END;

#Displays forecasted values 
BEGIN
SYS.RQSCRIPTDROP('Forecast2');
SYS.RQSCRIPTCREATE('Forecast2',
'function(stocks)
{
library(TTR)
ordered <- stocks[order(as.Date(stocks$S_DATE, format="%Y-%m-%d")),]
tstocks <- ts(ordered$CL,frequency=5)
ses1 = HoltWinters(tstocks,beta = FALSE, gamma = FALSE)
ses2 = HoltWinters(tstocks, gamma = FALSE)
ses3 = HoltWinters(tstocks)
pred1=predict(ses1,7)
pred2=predict(ses2,7)
pred3=predict(ses3,7)
dates=seq(as.Date(Sys.Date()+1), as.Date(Sys.Date()+7), by="days")
data.frame(dates)
pred=data.frame(pred1,pred2,pred3)
pred$date=dates
pred
}');
END;

#Creates minimum spanning tree plot
BEGIN
SYS.RQSCRIPTDROP('MSTPlot');
SYS.RQSCRIPTCREATE('MSTPlot',
'function(stocks)
{
library(plyr)
library(reshape2)
library(corrplot)
library(igraph)

stocks$return <- ave(stocks$CL, factor(stocks$TICKER), 
FUN=function(x) c(-1*(diff(log(x),)),NA))
stocks=na.omit(stocks)
tickers=data.frame(c(rep("AAPL",10),rep("AVP",10),rep("BAC",10),rep("GE",10),
rep("KO",10),rep("MCD",10),rep("MS",10),rep("ORCL",10),rep("PG",10),rep("TEVA",10)))
corr=ddply(stocks, .(stocks$TICKER), function(x) 	
	ddply(stocks, .(stocks$TICKER), function(y)
	corr=cor(x$return,y$return,method="pearson")
	)
)
cdata=cbind(tickers,corr)
names(cdata)[1] <- "ticker"
names(cdata)[2] <- "ticker2"
names(cdata)[3] <- "corr"
cdata$distance=sqrt(2*(1-cdata$corr));
DM=acast(cdata, ticker ~ ticker2, value.var="distance")
G <- graph.adjacency(DM,weighted=TRUE);
MST=minimum.spanning.tree(G,algorithm="prim");
MST <- simplify(as.undirected(MST))
plot(MST,vertex.shape="circle",vertex.color="lightpink",vertex.size=20,vertex.size=30,vertex.label.cex=0.7,vertex.label.font=2,vertex.label.color="black")
}');
END;

#Creates correlation splot
BEGIN
SYS.RQSCRIPTDROP('CorrPlot');
SYS.RQSCRIPTCREATE('CorrPlot',
'function(stocks)
{
library(plyr)
library(reshape2)
library(corrplot)
stocks$return <- ave(stocks$CL, factor(stocks$TICKER), 
FUN=function(x) c(-1*(diff(log(x),)),NA))
stocks=na.omit(stocks)
tickers=data.frame(c(rep("AAPL",10),rep("AVP",10),rep("BAC",10),rep("GE",10),
rep("KO",10),rep("MCD",10),rep("MS",10),rep("ORCL",10),rep("PG",10),rep("TEVA",10)))
corr=ddply(stocks, .(stocks$TICKER), function(x) 	
	ddply(stocks, .(stocks$TICKER), function(y)
	corr=cor(x$return,y$return,method="pearson")
	)
)
cdata=cbind(tickers,corr)
names(cdata)[1] <- "ticker"
names(cdata)[2] <- "ticker2"
names(cdata)[3] <- "corr"
CM=acast(cdata, ticker ~ ticker2, value.var="corr")
corrplot.mixed(CM, lower = "number", upper = "color")
}');
END;

#Creates positive wordcloud
BEGIN
SYS.RQSCRIPTDROP('posWordCloud');
SYS.RQSCRIPTCREATE('posWordCloud',
'function(sentiments)
{
library(wordcloud)
library(tm)
pos.words=sentiments[,c(4,6)]
myCorpus = Corpus(VectorSource(pos.words))
dtm = TermDocumentMatrix(myCorpus)
m = as.matrix(dtm)
v = sort(rowSums(m), decreasing=TRUE)
myNames <- names(v)
d = data.frame(word=myNames, freq=v)
wordcloud(d$word, d$freq, min.freq=50,scale=c(4,0.7),color=brewer.pal(n=9, "Greens")[6:9],max.words=100)
}');
END;

#Creates negative wordcloud
BEGIN
SYS.RQSCRIPTDROP('negWordCloud');
SYS.RQSCRIPTCREATE('negWordCloud',
'function(sentiments)
{
library(wordcloud)
library(tm)
neg.words=sentiments[,c(5,7)]
myCorpus = Corpus(VectorSource(neg.words))
dtm = TermDocumentMatrix(myCorpus)
m = as.matrix(dtm)
v = sort(rowSums(m), decreasing=TRUE)
myNames <- names(v)
d = data.frame(word=myNames, freq=v)
wordcloud(d$word, d$freq, min.freq=50,scale=c(4,0.7),color=brewer.pal(n=9, "Reds")[6:9],max.words=100)
}');
END;



