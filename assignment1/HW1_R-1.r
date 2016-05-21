v <- c(7,2,1,0,3,-1,-3,4)
v

A <- matrix(v, nrow=4, ncol=2)
A

AT <- t(A)
AT

product <- A%*% AT
product

product <- AT %*% A
product

solve(A %*% AT)
solve(AT %*% A)

v <- c(v, c(-2))
v

B <- matrix(v, nrow=3, ncol=3)
B

Binv = solve(B)
Binv

B %*% Binv
Binv %*% B

C <-eigen(B)$vectors
C

B %*% C
C %*% B
solve(C) %*% B %*% C

dimnames(B) <- list(c("row1", "row2", "row3"), c("col1", "col2", "col3"))
B

dFrame = data.frame(B)
dFrame

class(dFrame)


mydata = read.csv("2006Data.csv")
head(mydata)

plot(mydata$Power, mydata$Temperature, main="Power vs. Temperature")
plot(mydata$Hour, mydata$Power, main="Hour vs. Power")

boxplot(mydata$Power ~ mydata$Hour, main="Power x Hour Boxplot", xlab="Hour", ylab="Power")

intervals= cut(mydata$Temperature, 100)
storeMean = aggregate(mydata$Power, by=list(intervals), FUN=mean)
head(storeMean)
storeMax = aggregate(mydata$Power, by=list(intervals), FUN=max)
head(storeMax)
storeMin = aggregate(mydata$Power, by=list(intervals), FUN=min)
head(storeMin)

plot(storeMax, ylim=c(0, 110), xlab="Temperature Intervals", ylab="Power", main="Max., Min., and Mean Power Consumptions")
par(pch=17, col="blue")
points(storeMin)
par(pch=15, col="red")
points(storeMean)

mp = storeMean$Group.1
midpoints = (aggregate(as.data.frame(mp), by=list(mp), FUN=function(x) (as.numeric(substr(unlist(strsplit(as.character(x), ","))[1], 2, 10))+as.numeric(unlist(strsplit(unlist(strsplit(as.character(x), ","))[2], "]")[1])[1]))/2    ))$mp
head(midpoints)

cov(matrix(c(midpoints, storeMin$x), nrow=100, ncol=2))
cov(matrix(c(midpoints, storeMean$x), nrow=100, ncol=2))
cov(matrix(c(midpoints, storeMax$x), nrow=100, ncol=2))













