drawBox <- function (box) {
  lowerLeftX<-box[1]
  lowerLeftY<-box[2]
  upperRightX<-box[3]
  upperRightY<-box[4]
  
  lines (x=c(lowerLeftX, lowerLeftX), y=c(lowerLeftY, upperRightY))
  lines (x=c(lowerLeftX, upperRightX), y=c(upperRightY, upperRightY))
  lines (x=c(upperRightX, upperRightX), y=c(upperRightY, lowerLeftY))
  lines (x=c(upperRightX, lowerLeftX), y=c(lowerLeftY, lowerLeftY))
}

data<-read.csv ("clustered_points.csv", header=FALSE)
colnames(data)<-c("x", "y", "cluster")
cluster.ids<-sort (unique(data$cluster))

scatterplot.colors<-numeric(nrow(data))
barplot.colors<-numeric(length(cluster.ids))

for (i in 1:length(cluster.ids)) {
  scatterplot.colors[data$cluster == cluster.ids[i]] <- i 
  barplot.colors[i] <- i
}

par (mfrow=c(1,2))

plot (x=data$x, y=data$y, col=scatterplot.colors, pch=19, cex=0.08, xlab="X", ylab="Y", main="Clustered points")

if (file.exists ("cpdb.csv")) {
  cpdb<-read.csv ("cpdb.csv", header=FALSE)
  points (x=cpdb[,1], y=cpdb[,2], col="red", cex=0.5, pch=19)
}

if (file.exists ("boxes.csv")) {
  
  boxes<-read.csv ("boxes.csv", header=FALSE)
  
#  leftmostX <- min (boxes[,1])
#  bottomY <- min (boxes[,2])
#  rightmostX <- max (boxes[,3])
#  topY <- max (boxes[,4])
  
#  plot (x=c(leftmostX, rightmostX), y=c(bottomY, topY), col=0)
  
  for (row in 1:nrow(boxes)) {
    drawBox (boxes[row,])
  }
}

t<-table(data$cluster)
names(t)[which(names(t)=="0")]<-"Noise"
barplot (t, col=barplot.colors, main="Number of points in each cluster", xlab="Cluster ID", ylab="Number of points")
par(mar=c(0,0,0,0))
legend ("topleft", legend=t, col=barplot.colors, pch=19)



#print (paste ("There are", nrow(data),  "points", sep=" "))
