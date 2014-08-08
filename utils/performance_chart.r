x<-c(0.5, 1, 1.5, 2, 2.5, 3)

y6<-c(3.4, 6.3, 12, 21, 29, 44)
y5<-c(3.9, 7.2, 14, 22, 36, 57)
y4<-c(3.9, 7.9, 16, 25, 40, 66)

y4_2<-c(4.3, 9.2, 17, 22, 40, 51)
y5_2<-c(4, 7.8, 14, 20, 30, 41)
y6_2<-c(3.7, 6.5, 13, 18, 25, 33)

chart.cex<-1.5
lwd.2<-2
col.4.1<-"#B0B0B0"
col.5.1<-"#FFD5B0"
col.6.1<-"#D0D0FF"
col.4.2<-"black"
col.5.2<-"orange"
col.6.2<-"blue"
pch.4<-17
pch.5<-15
pch.6<-19

plot (x, y4, pch=pch.4, cex=chart.cex, col=col.4.1, xlab="Data set size, millions of records", ylab="Time taken, minutes")
lines (x, y4, col=col.4.1)

points(x, y5, col=col.5.1, pch=pch.5, cex=chart.cex)
lines (x, y5, col=col.5.1)

points(x, y6, col=col.6.1, pch=pch.6, cex=chart.cex)
lines(x,y6, col=col.6.1)

points(x, y4_2, col=col.4.2, pch=pch.4, cex=chart.cex)
lines(x,y4_2, col=col.4.2, lwd=lwd.2)

points(x, y5_2, col=col.5.2, pch=pch.5, cex=chart.cex)
lines (x, y5_2, col=col.5.2, lwd=lwd.2)

points(x, y6_2, col=col.6.2, pch=pch.6, cex=chart.cex)
lines (x, y6_2, col=col.6.2, lwd=lwd.2)

legend ("topleft",
        legend=c("Version 0.0.1 on 4 worker nodes", "0.0.1 on 5 nodes", "0.0.1 on 6 nodes", "Version 0.0.2 on 4 worker nodes", "0.0.2 on 5 nodes", "0.0.2 on 6 nodes"),
        col=c(col.4.1, col.5.1, col.6.1, col.4.2, col.5.2, col.6.2), pch=c(pch.4, pch.5, pch.6, pch.4, pch.5, pch.6))