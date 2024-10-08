#######################################
#  启动数据中心后台服务程序的脚本
#  不能一条一条命令输入去启动程序
#  通过编写shell脚本去启动
#######################################


#  检查服务程序是否超时，配置在/etc/rc.local中由root用户执行
#  /project/tools2/bin/procctl 30 /project/tools1/bin/checkproc


#  压缩数据中心后台服务程序的备份日志
/project/tools2/bin/procctl 300 /project/tools2/bin/gzipfiles /log/idc "*.log.20*" 0.04


#  生成用于测试的全国气象站点观测的分钟数据
/project/tools2/bin/procctl 60 /project/idc/bin/crtsurfdata /project/idc/ini/stcode.ini /tmp/idc/surfdata /log/idc/crtsurfdata.log xml,json,csv


#  清理原始的全国气象站点观测的分钟数据目录 /tmp/idc/surfdata 中的历史数据文件
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /tmp/idc/surfdata "*" 0.04


#  下载全国气象站点观测的分钟数据的xml文件
#  不需要测试用的当ptype=3时候的remoteparhbak参数了
/project/tools2/bin/procctl 30 /project/tools2/bin/ftpgetfiles /log/idc/ftpgetfiles_surfdata.log "<host>127.0.0.1:21</host><mode>1</mode><username>lulu</username><passward>2333332</passward><localpath>/idcdata/surfdata</localpath><remotepath>/tmp/idc/surfdata</remotepath><matchname>SURF_ZH*.XML</matchname><listfilename>/idcdata/ftplist/ftpgetfiles_surfdata.list</listfilename><ptype>1</ptype><okfilename>/idcdata/ftplist/ftpgetfiles_surfdata.xml</okfilename><checkmtime>true</checkmtime><timeout>80</timeout><pname>ftpgetfiles_surfdata</pname>" 


# 上传全国气象站点观测的分钟数据的xml文件
/project/tools2/bin/procctl 30 /project/tools2/bin/ftpputfiles /log/idc/ftpputfiles_surfdata.log "<host>127.0.0.1:21</host><mode>1</mode><username>lulu</username><passward>2333332</passward><localpath>/tmp/idc/surfdata</localpath><remotepath>/tmp/ftpputest</remotepath><matchname>SURF_ZH*.JSON</matchname><ptype>1</ptype><okfilename>/idcdata/ftplist/ftpputfiles_surfdata.xml</okfilename><timeout>80</timeout><pname>ftpputfiles_surfdata</pname>"


# 数据生成程序会一直生成文件，然后一直下载，一直上传
# 会导致从ftp下载和上传的目录被撑满
# 因此需要清理这两个文件夹中的历史文件
# 清理采集的全国气象站点观测的分钟数据目录 /idcdata/surfdata 中的历史数据文件
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /idcdata/surfdata "*" 0.04

# 清理采集的全国气象站点观测的分钟数据目录 /tmp/ftpputest 中的历史数据文件
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /tmp/ftpputest "*" 0.04


#tcp文件传输的服务端程序
/project/tools2/bin/procctl 10 /project/tools2/bin/fileserver 5005 /log/idc/fileserver.log


#把使用ftp上传的文件（在目录/tmp/ftpputest）中的文件使用tcp文件传输方式上传到/tmp/tcpputest目录中
#地址使用127.0.0.1防止虚拟机的地址出现变化
/project/tools2/bin/procctl 20 /project/tools2/bin/tcpputfiles /log/idc/tcpputfiles_surfdata.log "<ip>127.0.0.1</ip><port>5005</port><ptype>1</ptype><clientpath>/tmp/ftpputest</clientpath><andchild>true</andchild><matchname>*.XML,*.CSV,*.JSON</matchname><srvpath>/tmp/tcpputest</srvpath><timetvl>10</timetvl><timeout>50</timeout><pname>tcpputfiles_surfdata</pname>"


#把目录/tmp/tcpputest中的文件使用tcp文件传输的方式下载到/tmp/tcpgetest目录中
/project/tools2/bin/procctl 20 /project/tools2/bin/tcpgetfiles /log/idc/tcpgetfiles_surfdata.log "<ip>127.0.0.1</ip><port>5005</port><ptype>1</ptype><srvpath>/tmp/tcpputest</srvpath><andchild>true</andchild><matchname>*.XML,*.CSV,*.JSON</matchname><clientpath>/tmp/tcpgetest</clientpath><timetvl>10</timetvl><timeout>50</timeout><pname>tcpgetfiles_surfdata</pname>"



#清理采集的全国气象站点观测的分钟数据目录/tmp/tcpgetest中的历史数据文件,防止文件堆积
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /tmp/tcpgetest "*" 0.04


#把全国站点参数保存到数据库表中，如果站点不存在则插入，站点已存在则更新
/project/tools2/bin/procctl 120 /project/idc2/bin/obtcodetodb /project/idc/ini/stcode.ini "127.0.0.1,root,123456,mysql,3306" utf8 /log/idc/obtcodetodb.log


#把全国站点分钟观测数据保存到数据库表的T_ZHOBTMIND表中，数据只插入不更新
/project/tools2/bin/procctl 120 /project/idc2/bin/obtmindtodb /idcdata/surfdata "127.0.0.1,root,123456,mysql,3306" utf8 /log/idc/obtmindtodb.log


#清理T_ZHOBTMIND表和T_ZHOBTMIND1表中120分之前的数据，防止磁盘空间被撑满
#/project/tools2/bin/procctl 120 /project/tools2/bin/execsql /project/idc/sql/cleardata.sql "127.0.0.1,root,123456,mysql,3306" utf8 /log/idc/execsql.log
#有了数据管理子系统中数据清理的deletetable程序，这个程序已经不需要了


#每隔一小时把T_ZHOBTCODE表中的全部数据抽取出来
/project/tools2/bin/procctl 3600 /project/tools2/bin/dminingmysql /log/idc/dminingmysql_ZHOBTCODE.log "<connstr>127.0.0.1,root,123456,mysql,3306</connstr><charset>utf8</charset><selectsql>select obtid,cityname,provname,lat,lon,height from T_ZHOBTCODE</selectsql><fieldstr>obtid,cityname,provname,lat,lon,height</fieldstr><fieldlen>10,30,30,10,10,10</fieldlen><bfilename>ZHOBTCODE</bfilename><efilename>HYCZ</efilename><outpath>/idcdata/dmindata</outpath><timeout>30</timeout><pname>dminingmysql_ZHOBTCODE</pname>" 


#每30秒从T_ZHOBTMIND表中把分钟观测数据增量抽取出来，并把最大自增字段保存到本地数据库表中
/project/tools2/bin/procctl 30 /project/tools2/bin/dminingmysql /log/idc/dminingmysql_ZHOBTMIND.log "<connstr>127.0.0.1,root,123456,mysql,3306</connstr><charset>utf8</charset><selectsql>select obtid,date_format(ddatetime,'%%Y-%%m-%%d %%H:%%m:%%s'),t,p,u,wd,wf,r,vis,keyid from T_ZHOBTMIND where keyid>:1 and ddatetime>timestampadd(minute,-120,now())</selectsql><fieldstr>obtid,ddatetime,t,p,u,wd,wf,r,vis,keyid</fieldstr><fieldlen>10,19,8,8,8,8,8,8,8,15</fieldlen><bfilename>ZHOBTMIND</bfilename><efilename>HYCZ</efilename><outpath>/idcdata/dmindata</outpath><incfield>keyid</incfield><timeout>30</timeout><pname>dminingmysql_ZHOBTMIND_HYCZ</pname><maxcount>1000</maxcount><connstr1>127.0.0.1,root,123456,mysql,3306</connstr1>" 



#清理/idcdata/dmindata目录中的历史文件，防止磁盘空间被撑满
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /idcdata/dmindata "*" 0.04


#使用tcp文件上传程序，把/idcdata/dmindata目录中的xml发送到/idcdata/xmltodb/vip1目录，交给xmltodb程序去入库
/project/tools2/bin/procctl 20 /project/tools2/bin/tcpputfiles /log/idc/tcpputfiles_dmindata.log "<ip>127.0.0.1</ip><port>5005</port><ptype>1</ptype><clientpath>/idcdata/dmindata</clientpath><andchild>false</andchild><matchname>*.XML</matchname><srvpath>/idcdata/xmltodb/vip1</srvpath><timetvl>10</timetvl><timeout>50</timeout><pname>tcpputfiles_dmindata</pname>"


#把/idcdata/xmltodb/vip1目录下的xml文件入库
/project/tools2/bin/procctl 10 /project/tools2/bin/xmltodb /log/idc/xmltodb_vip1.log "<connstr>127.0.0.1,root,123456,mysql,3306</connstr><charset>utf8</charset><inifilename>/project/idc2/ini/xmltodb.xml</inifilename><xmlpath>/idcdata/xmltodb/vip1</xmlpath><xmlpathbak>/idcdata/xmltodb/vip1bak</xmlpathbak><xmlpatherr>/idcdata/xmltodb/vip1err</xmlpatherr><timetvl>5</timetvl><timeout>50</timeout><pname>xmltodb_vip1</pname>" 


#清理/idcdata/xmltodb/vip1bak和/idcdata/xmltodb/vip1err目录中的文件，防止把空间撑满
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /idcdata/xmltodb/vip1bak "*" 0.04
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /idcdata/xmltodb/vip1err "*" 0.04


#采用全表刷新同步的方法，把表T_ZHOBTCODE1中的数据同步到表T_ZHOBTCODE2中
/project/tools2/bin/procctl 10 /project/tools2/bin/syncupdate /log/idc/syncupdate_ZHOBTCODE2.log "<localconnstr>192.168.84.136,root,123456,mysql,3306</localconnstr><charset>utf8</charset><fedtname>LK_ZHOBTCODE1</fedtname><localtname>T_ZHOBTCODE2</localtname><remotecols>obtid,cityname,provname,lat,lon,height/10,upttime,keyid</remotecols><localcols>stid,cityname,provname,lat,lon,altitude,upttime,keyid</localcols><synctype>1</synctype><timeout>50</timeout><pname>syncupdate_ZHOBTCODE2</pname>" 

#采用分批同步的方法，把T_ZHOBTCODE1中obtid like '54%'的记录同步到表T_ZHOBTCODE3
/project/tools2/bin/procctl 10 /project/tools2/bin/syncupdate /log/idc/syncupdate_ZHOBTCODE3.log "<localconnstr>192.168.84.136,root,123456,mysql,3306</localconnstr><charset>utf8</charset><fedtname>LK_ZHOBTCODE1</fedtname><localtname>T_ZHOBTCODE3</localtname><remotecols>obtid,cityname,provname,lat,lon,height/10,upttime,keyid</remotecols><localcols>stid,cityname,provname,lat,lon,altitude,upttime,keyid</localcols><where>where obtid like '54%%'</where><synctype>2</synctype><remoteconnstr>192.168.84.136,root,123456,mysql,3306</remoteconnstr><remotetname>T_ZHOBTCODE1</remotetname><remotekeycol>obtid</remotekeycol><localkeycol>stid</localkeycol><maxcount>10</maxcount><timeout>50</timeout><pname>syncupdate_ZHOBTCODE3</pname>" 



#采用增量同步的方法，把T_ZHOBTMIND1中全部的记录同步到表T_ZHOBTMIND2
/project/tools2/bin/procctl 10 /project/tools2/bin/syncincrement /log/idc/syncincrement_ZHOBTMIND2.log "<localconnstr>192.168.84.136,root,123456,mysql,3306</localconnstr><remoteconnstr>192.168.84.136,root,123456,mysql,3306</remoteconnstr><charset>utf8</charset><remotetname>T_ZHOBTMIND1</remotetname><fedtname>LK_ZHOBTMIND1</fedtname><localtname>T_ZHOBTMIND2</localtname><remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols><localcols>stid,ddatetime,t,p,u,wd,wf,r,vis,upttime,recid</localcols><remotekeycol>keyid</remotekeycol><localkeycol>recid</localkeycol><maxcount>300</maxcount><timetvl>2</timetvl><timeout>50</timeout><pname>syncincrement_ZHOBTMIND2</pname>"


#采用增量同步的方法，把表T_ZHOBTMIND1中obtid like '54%'的记录同步到表T_ZHOBTMIND3
/project/tools2/bin/procctl 10 /project/tools2/bin/syncincrement /log/idc/syncincrement_ZHOBTMIND3.log "<localconnstr>192.168.84.136,root,123456,mysql,3306</localconnstr><remoteconnstr>192.168.84.136,root,123456,mysql,3306</remoteconnstr><charset>utf8</charset><remotetname>T_ZHOBTMIND1</remotetname><fedtname>LK_ZHOBTMIND1</fedtname><localtname>T_ZHOBTMIND3</localtname><remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols><localcols>stid,ddatetime,t,p,u,wd,wf,r,vis,upttime,recid</localcols><where>and obtid like '54%%'</where><remotekeycol>keyid</remotekeycol><localkeycol>recid</localkeycol><maxcount>300</maxcount><timetvl>2</timetvl><timeout>50</timeout><pname>syncincrement_ZHOBTMIND3</pname>" 


#把T_ZHOBTMIND表中120分钟之前的数据迁移到T_ZHOBTMIND_HIS表中
/project/tools2/bin/procctl 3600 /project/tools2/bin/migratetable /log/idc/migratetable_ZHOBTMIND.log "<connstr>192.168.84.136,root,123456,mysql,3306</connstr><srctname>T_ZHOBTMIND</srctname><dsttname>T_ZHOBTMIND_HIS</dsttname><keycol>keyid</keycol><where>where ddatetime<timestampadd(minute,-120,now())</where><maxcount>300</maxcount><timeout>120</timeout><pname>migratetable_ZHOBTMIND</pname>"

#清理T_ZHOBTMIND1表中120分钟之前的数据
/project/tools2/bin/procctl 3600 /project/tools2/bin/deletetable /log/idc/deletetable_ZHOBTMIND1.log "<connstr>192.168.84.136,root,123456,mysql,3306</connstr><tname>T_ZHOBTMIND1</tname><keycol>keyid</keycol><where>where ddatetime<timestampadd(minute,-120,now())</where><timeout>120</timeout><pname>deletetable_ZHOBTMIND1</pname>" 

#清理T_ZHOBTMIND2表中120分钟之前的数据
/project/tools2/bin/procctl 3600 /project/tools2/bin/deletetable /log/idc/deletetable_ZHOBTMIND2.log "<connstr>192.168.84.136,root,123456,mysql,3306</connstr><tname>T_ZHOBTMIND2</tname><keycol>recid</keycol><where>where ddatetime<timestampadd(minute,-120,now())</where><timeout>120</timeout><pname>deletetable_ZHOBTMIND2</pname>" 

#清理T_ZHOBTMIND3表中120分钟之前的数据
/project/tools2/bin/procctl 3600 /project/tools2/bin/deletetable /log/idc/deletetable_ZHOBTMIND3.log "<connstr>192.168.84.136,root,123456,mysql,3306</connstr><tname>T_ZHOBTMIND3</tname><keycol>recid</keycol><where>where ddatetime<timestampadd(minute,-120,now())</where><timeout>120</timeout><pname>deletetable_ZHOBTMIND3</pname>" 

#清理T_ZHOBTMIND_HIS表中240分钟之前的数据
/project/tools2/bin/procctl 3600 /project/tools2/bin/deletetable /log/idc/deletetable_ZHOBTMIND_HIS.log "<connstr>192.168.84.136,root,123456,mysql,3306</connstr><tname>T_ZHOBTMIND_HIS</tname><keycol>keyid</keycol><where>where ddatetime<timestampadd(minute,-240,now())</where><timeout>120</timeout><pname>deletetable_ZHOBTMIND_HIS</pname>" 



#增加数据抽取程序
#每隔1小时把T_ZHOBTCODE表中全部的数据抽取出来，放在/idcdata/xmltodb/vip2目录，给xmltodb_oracle入库到Oracle
/project/tools2/bin/procctl 3600 /project/tools2/bin/dminingmysql /log/idc/dminingmysql_ZHOBTCODE_vip2.log "<connstr>127.0.0.1,root,123456,mysql,3306</connstr><charset>utf8</charset><selectsql>select obtid,cityname,provname,lat,lon,height from T_ZHOBTCODE</selectsql><fieldstr>obtid,cityname,provname,lat,lon,height</fieldstr><fieldlen>10,30,30,10,10,10</fieldlen><bfilename>ZHOBTCODE</bfilename><efilename>HYCZ</efilename><outpath>/idcdata/xmltodb/vip2</outpath><timeout>30</timeout><pname>dminingmysql_ZHOBTCODE_vip2</pname>"


#每隔30秒把T_ZHOBTMIND表中的数据增量抽取出来，放在/idcdata/xmltodb/vip2目录，给xmltodb_oracle入库到Oracle
/project/tools2/bin/procctl 30 /project/tools2/bin/dminingmysql /log/idc/dminingmysql_ZHOBTMIND_vip2.log "<connstr>127.0.0.1,root,123456,mysql,3306</connstr><charset>utf8</charset><selectsql>select obtid,date_format(ddatetime,'%%Y-%%m-%%d %%H:%%m:%%s'),t,p,u,wd,wf,r,vis,keyid from T_ZHOBTMIND where keyid>:1 and ddatetime>timestampadd(minute,-120,now())</selectsql><fieldstr>obtid,ddatetime,t,p,u,wd,wf,r,vis,keyid</fieldstr><fieldlen>10,19,8,8,8,8,8,8,8,15</fieldlen><bfilename>ZHOBTMIND</bfilename><efilename>HYCZ</efilename><outpath>/idcdata/xmltodb/vip2</outpath><incfield>keyid</incfield><timeout>30</timeout><pname>dminingmysql_ZHOBTMIND_vip2</pname><maxcount>1000</maxcount><connstr1>127.0.0.1,root,123456,mysql,3306</connstr1>" 


#清理/idcdata/xmltodb/vip2bak和/idcdata/xmltodb/vip2err目录中的文件，防止把空间撑满
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /idcdata/xmltodb/vip2    "*" 0.04
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /idcdata/xmltodb/vip2bak "*" 0.04
/project/tools2/bin/procctl 300 /project/tools2/bin/deletefiles /idcdata/xmltodb/vip2err "*" 0.04


#把/idcdata/xmltodb/vip2目录中的xml文件入库到Oracle数据库的表中
/project/tools1/bin/procctl 10 /project/tools2/bin/xmltodb_oracle /log/idc/xmltodb_oracle_vip2.log "<connstr>qxidc/qxidcpwd@snorcl11g_136</connstr><charset>Simplified Chinese_China.AL32UTF8</charset><inifilename>/project/idc2/ini/xmltodb.xml</inifilename><xmlpath>/idcdata/xmltodb/vip2</xmlpath><xmlpathbak>/idcdata/xmltodb/vip2bak</xmlpathbak><xmlpatherr>/idcdata/xmltodb/vip2err</xmlpatherr><timetvl>5</timetvl><timeout>50</timeout><pname>xmltodb_oracle_vip2</pname>"

#把T_ZHOBTMIND1表中0.03天之前的数据迁移到T_ZHOBTMIND_HIS表中
/project/tools2/bin/procctl 3600 /project/tools2/bin/migratetable_oracle /log/idc/migratetable_oracle_ZHOBTMIND1.log "<connstr>qxidc/qxidcpwd@snorcl11g_136</connstr><srctname>T_ZHOBTMIND1</srctname><dsttname>T_ZHOBTMIND_HIS</dsttname><keycol>rowid</keycol><where>where ddatetime<sysdate-0.03</where><maxcount>300</maxcount><timeout>120</timeout><pname>migratetable_oracle_ZHOBTMIND1</pname>"

#清理T_ZHOBTMIND_HIS表0.05天之前的数据
/project/tools2/bin/procctl 3600 /project/tools2/bin/deletetable_oracle /log/idc/deletetable_oracle_ZHOBTMIND_HIS.log "<connstr>qxidc/qxidcpwd@snorcl11g_136</connstr><tname>T_ZHOBTMIND_HIS</tname><keycol>rowid</keycol><where>where ddatetime<sysdate-0.05</where><timeout>120</timeout><pname>deletetable_oracle_ZHOBTMIND_HIS</pname>"



#把qxidc.T_ZHOBTCODE1@db136表中的数据同步到scott.T_ZHOBTCODE2
/project/tools2/bin/syncupdate_oracle /log/idc/syncupdate_oracle_ZHOBTCODE2.log "<localconnstr>scott/2333332@snorcl11g_136</localconnstr><charset>Simplified Chinese_China.AL32UTF8</charset><lnktname>T_ZHOBTCODE1@db136</lnktname><localtname>T_ZHOBTCODE2</localtname><remotecols>obtid,cityname,provname,lat,lon,height,upttime,keyid</remotecols><localcols>obtid,cityname,provname,lat,lon,height,upttime,keyid</localcols><synctype>1</synctype><timeout>50</timeout><pname>syncupdate_oracle_ZHOBTCODE2</pname>"


#把qxidc.T_ZHOBTMIND1@db136表中的数据同步到scott.T_ZHOBTMIND2
/project/tools2/bin/procctl 10 /project/tools2/bin/syncincrement_oracle /log/idc/syncincrement_oracle_ZHOBTMIND2.log "<localconnstr>scott/2333332@snorcl11g_136</localconnstr><remoteconnstr>qxidc/qxidcpwd@snorcl11g_136</remoteconnstr><charset>Simplified Chinese_China.AL32UTF8</charset><remotetname>T_ZHOBTMIND1</remotetname><lnktname>T_ZHOBTMIND1@db136</lnktname><localtname>T_ZHOBTMIND2</localtname><remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols><localcols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</localcols><remotekeycol>keyid</remotekeycol><localkeycol>keyid</localkeycol><maxcount>300</maxcount><timetvl>2</timetvl><timeout>50</timeout><pname>syncincrement_oracle_ZHOBTMIND2</pname>"


#清理scott.T_ZHOBTMIND2表中0.05天之前的数据
/project/tools2/bin/procctl 3600 /project/tools2/bin/deletetable_oracle /log/idc/deletetable_oracle_ZHOBTMIND2.log "<connstr>scott/2333332@snorcl11g_136</connstr><tname>T_ZHOBTMIND2</tname><keycol>rowid</keycol><where>where ddatetime<sysdate-0.05</where><timeout>120</timeout><pname>deletetable_oracle_ZHOBTMIND2</pname>"









