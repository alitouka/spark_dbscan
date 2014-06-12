data<-read.csv ("histogram.csv", header=FALSE)
colnames(data)<-c("from", "to", "value")
sort.order<-order(data$from)
data<-data[sort.order,]
num.rows = nrow(data)
one.before.last.row = num.rows-1
decimal.places=4
bar.names<-paste("[", round(data$from[1:one.before.last.row], decimal.places), ", ", round (data$to[1:one.before.last.row], decimal.places), ")")
bar.names<-c(bar.names, paste("[", round (data$from[num.rows], decimal.places) , ", ", round (data$to[num.rows], decimal.places), "]"))
par (mar=c(6, 11, 2, 2))
barplot (data$value, names.arg=bar.names, las=2, horiz=TRUE)
