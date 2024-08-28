/*
 *   obtcodetodb.cpp 本程序用于把全国数据站点参数数据保存到数据库T_ZHOBTCODE表中
 *
*/

#include "_public.h"
#include "_mysql.h"
#include "signal.h"


//全国气象站点参数结构体
struct st_stcode
{
  char   provname[31];  //省
  char   obtid[11];     //站号
  char   cityname[31];   //站名
  char   lat[11];       //纬度
  char   lon[11];       //经度
  char   height[11];    //海拔高度
};


//存放全国气象站点参数的容器
vector<struct st_stcode> vstcode;


//把站点参数文件加载到vstcode容器中
bool LoadSTCode(const char *inifile);


//创建全局的日志文件对象
CLogFile logfile;


//创建数据库连接对象
connection conn;


//进程心跳
CPActive PActive;


//退出信号的处理函数
void EXIT(int sig);

int main(int argc, char *argv[])
{
  //帮助文档
  if (argc!=5)
  {
    printf("\n");
    printf("Using:./obtcodetodb inifile connstr charset logfile\n");

    printf("Example:/project/tools2/bin/procctl 120 /project/idc2/bin/obtcodetodb /project/idc/ini/stcode.ini \"127.0.0.1,root,123456,mysql,3306\" utf8 /log/idc/obtcodetodb.log\n\n");

    printf("本程序用于把全国站点参数数据保存到数据库表中，如果站点不存在则插入，站点已存在则更新\n");
    printf("inifile 站点参数文件名（全路径）\n");
    printf("connstr 数据库连接参数：ip,username,password,dbname,port\n");
    printf("charset 数据库的字符集\n");
    printf("logfile 本程序运行的日志文件\n");
    printf("程序每120秒运行一次，由procctl调度。\n\n\n");
    //120秒是看业务需求，站点参数要是一天或者隔很久更新一次，120秒足够

    return -1;
  }


  //处理程序退出的信号
  //关闭全部的信号和输入输出
  //设置信号，在shell状态下可用"kill+进程号"正常终止这些进程
  //但请不要用"kill -9 +进程号"强行终止
  CloseIOAndSignal(); signal(SIGINT,EXIT); signal(SIGTERM,EXIT);


  //打开日志文件
  if (logfile.Open(argv[4],"a+")==false)
  {
     printf("打开日志文件失败(%s)\n",argv[4]); return -1;
  }

  //进程的心跳，10秒足够
  PActive.AddPInfo(10,"obtcodetodb");
  //注意，在调试程序时，可以启用类似以下的代码，将心跳时间设置得长一些，防止超时
  //PActive.AddPInfo(500,"obtcodetodb");



  //把全国站点参数文件加载到vstcode容器中;也可以打开文件，用循环一条一条处理文件内容
  if (LoadSTCode(argv[1])==false) return -1;

  //日志信息，提示已经加载到容器中
  logfile.Write("加载参数文件（%s）成功，站点数（%d）\n",argv[1],vstcode.size());


  //连接数据库，连接数据库的代码要放在加载参数文件之后，如果加载参数文件失败，都不用去连接数据库了
  if (conn.connecttodb(argv[2],argv[3])!=0)
  {
    //连接失败写日志
    logfile.Write("connect database(%s) failed.\n%s\n",argv[2],conn.m_cda.message); return -1;
  }

  //连接成功给提示
  logfile.Write("connect database(%s) ok.\n",argv[2]);


  //声明结构体，绑定输入参数的地址
  struct st_stcode stcode;  

  //准备插入表的SQL语句
  sqlstatement stmtins(&conn);
  //更新时间使用now()，没有使用缺省值，是为了兼容性考虑，其他数据库不一定有这个
  stmtins.prepare("insert into T_ZHOBTCODE(obtid,cityname,provname,lat,lon,height,upttime) values(:1,:2,:3,:4*100,:5*100,:6*10,now())");  
  

  //绑定输入参数的地址
  stmtins.bindin(1,stcode.obtid,10);
  stmtins.bindin(2,stcode.cityname,30);
  stmtins.bindin(3,stcode.provname,30);
  stmtins.bindin(4,stcode.lat,10);
  stmtins.bindin(5,stcode.lon,10);
  stmtins.bindin(6,stcode.height,10);


  //准备更新表的SQL语句
  sqlstatement stmtupt(&conn);
  stmtupt.prepare("update T_ZHOBTCODE set cityname=:1,provname=:2,lat=:3*100,lon=:4*100,height=:5*10,upttime=now() where obtid=:6");

  //绑定输入参数的地址
  stmtupt.bindin(1,stcode.cityname,30);
  stmtupt.bindin(2,stcode.provname,30);
  stmtupt.bindin(3,stcode.lat,10);
  stmtupt.bindin(4,stcode.lon,10);
  stmtupt.bindin(5,stcode.height,10);
  stmtupt.bindin(6,stcode.obtid,10);


  //插入记录数、更新记录数，初始化为0
  int inscount=0,uptcount=0;

  //计时器，记录完成下面的步骤消耗了多长时间
  CTimer Timer;


  //遍历vstcode容器
  for (int ii=0;ii<vstcode.size();ii++)
  {
     //从容器中取出一条记录到结构体stcode中
     memcpy(&stcode,&vstcode[ii],sizeof(struct st_stcode));

     //执行插入的SQL语句
     if (stmtins.execute()!=0)
     {
       //如果记录已存在，执行更新的SQL语句
       //判断记录已经存在的方法是判断插入SQL语句的返回值，如果返回值是1062，说明记录已存在
       if (stmtins.m_cda.rc==1062)
       {
         if (stmtupt.execute()!=0)
         {
            //更新失败，写日志
            logfile.Write("stmtupt.execute() failed.\n%s\n%s\n",stmtupt.m_sql,stmtupt.m_cda.message); return -1;
         }
         else
         {
            //更新成功，更新的记录数+1
            uptcount++;
         }
       }
       else
       {
        //插入失败的代码不是1062，记录日志，退出
        logfile.Write("stmtins.execute() failed.\n%s\n%s\n",stmtins.m_sql,stmtins.m_cda.message); return -1;
       }
     }
     else
     {
       //插入成功，插入的记录数+1
       inscount++;
     }
  }

  //把总记录数，插入记录数，更新记录数，消耗时长记录日志
  logfile.Write("总记录数=%d，插入数=%d，更新数=%d，耗时=%.2f\n",vstcode.size(),inscount,uptcount,Timer.Elapsed());


  //提交事务
  conn.commit();


  return 0;
}


//把站点参数文件加载到vstcode容器中
bool LoadSTCode(const char *inifile)
{
  //文件操作对象
  CFile File;

  //打开站点参数文件
  if (File.Open(inifile,"r")==false)
  {
     logfile.Write("File.Open(%s) failed.\n",inifile); return false;
  }

  char strBuffer[301];

  CCmdStr CmdStr;

  struct st_stcode stcode;

  while(true)
  {
    //从站点文件中读取一行，如果已读取完，跳出循环
    if (File.Fgets(strBuffer,300,true)==false) break;
    
    //把读取到的一行拆分
    CmdStr.SplitToCmd(strBuffer,",",true);

    //扔掉无效的行
    if (CmdStr.CmdCount()!=6) continue;

    //把站点参数的每个数据保存到站点参数的结构体中
    memset(&stcode,0,sizeof(struct st_stcode));
    CmdStr.GetValue(0, stcode.provname,30);    //省
    CmdStr.GetValue(1, stcode.obtid,10);       //站号
    CmdStr.GetValue(2, stcode.cityname,30);     //站名
    CmdStr.GetValue(3, stcode.lat,10);         //纬度
    CmdStr.GetValue(4, stcode.lon,10);         //经度
    CmdStr.GetValue(5, stcode.height,10);      //海拔高度

    //把站点参数结构体放入站点参数容器
    vstcode.push_back(stcode);
  }

  return true;
}


void EXIT(int sig)
{
  logfile.Write("程序退出，sig=%d\n\n",sig);

  //增加断开数据库连接的代码
  conn.disconnect();

  exit(0);
}
