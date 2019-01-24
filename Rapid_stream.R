##---
## Title: "Rapid stream connectivity based on geofabric data"
## Author: Ning Liu
## Email:N.LIU@murdoch.edu.au
##---

# parrale function for getting connectivity
f_upstream_prarallel<-function(i,catchment_info=da,maxup=5){
    listall<-rep(0,maxup+3)
    hydroid<- catchment_info$HydroID[i]
     list_upstreamID = catchment_info$HydroID[catchment_info$NextDownID==hydroid]
    # count the total number of the upstreams
    count_upstream = length(list_upstreamID)
     nextDownID = catchment_info$NextDownID[catchment_info$HydroID==hydroid]
     if(nextDownID<0) nextDownID<-0
    listup<-c(hydroid,nextDownID,count_upstream,list_upstreamID)

    listall[c(1:length(listup))]<-listup
    listall
}
load("2016SI/rapid/data/processingData/afghCatchment.RData")
AU_AHGF<-geofabric@data
AU_AHGF<-AU_AHGF[c("HydroID","NextDownID","SegmentNo")]
AU_AHGF$HydroID<-as.integer(as.character(AU_AHGF$HydroID))
AU_AHGF$NextDownID<-as.integer(as.character(AU_AHGF$NextDownID))

t_start<-Sys.time()
t_start
maxups=4

upstream<-mclapply(c(1:10000),f_upstream_prarallel,catchment_info=AU_AHGF[1:10000,],maxup=maxups,mc.cores = 16)
Sys.time()-t_start

upstream<-do.call(rbind,upstream)

ups<-paste("UP_",c(1:maxups),sep="")
colnames(upstream)<-c("HydroID","NextDownID","Count",ups)
print(paste("max upstream NO. = ",max(upstream[,"Count"])))
head(upstream)
tail(upstream)
Sys.time()
# Write stream connection
write.table(upstream,"rapid_connect.csv",row.names = F,col.names = F,sep=",")