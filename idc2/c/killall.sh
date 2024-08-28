####################################
#  停止数据中心后台服务程序的脚本
####################################


#必须要先终止调度程序，如果先终止服务程序，
#可能会产生一种情况就是把服务程序终止后，调度程序又会重新把服务程序启动

#使用-9的参数杀掉调度程序
killall -9 procctl

#使用killall每个服务程序
killall gzipfiles crtsurfdata deletefiles ftpgetfiles ftpputfiles tcpputfiles tcpgetfiles fileserver
killall obtcodetodb obtmindtodb execsql dminingmysql xmltodb syncupdate syncincrement
killall deletetable migratetable xmltodb_oracle migratetable_oracle deletetable_oracle 
killall syncupdate_oracle syncincrement_oracle

#让服务程序有时间可以退出
sleep 3

#再使用-9参数杀掉所有的程序，强制退出
killall -9 gzipfiles crtsurfdata deletefiles ftpgetfiles ftpputfiles tcpputfiles tcpgetfiles fileserver
killall -9 obtcodetodb obtmindtodb execsql dminingmysql xmltodb syncupdate syncincrement
killall -9 deletetable migratetable xmltodb_oracle migratetable_oracle deletetable_oracle
killall -9 syncupdate_oracle syncincrement_oracle
