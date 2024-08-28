/*
 *   obtmindtodb.cpp 本程序用于把全国站点分钟观测数据保存到数据库T_ZHOBTMIND表中，支持xml和csv两种文件格式
 *
*/

#include "idcapp.h"
#include "signal.h"


//创建全局的日志文件对象
CLogFile logfile;


//创建数据库连接对象
connection conn;


//进程心跳
CPActive PActive;


//退出信号的处理函数
void EXIT(int sig);


//业务处理主函数
bool _obtmindtodb(char *pathname,char *connstr,char *charset);


int main(int argc, char *argv[])
{
  //帮助文档
  if (argc!=5)
  {
    printf("\n");
    printf("Using:./obtmindtodb pathname connstr charset logfile\n");

    printf("Example:/project/tools2/bin/procctl 10 /project/idc2/bin/obtmindtodb /idcdata/surfdata \"127.0.0.1,root,123456,mysql,3306\" utf8 /log/idc/obtmindtodb.log\n\n");

    printf("本程序用于把全国站点分钟观测数据保存到数据库的T_ZHOBTMIND表中，数据只插入，不更新\n");
    printf("pathname 全国站点分钟观测数据文件存放的目录\n");
    printf("connstr 数据库连接参数：ip,username,password,dbname,port\n");
    printf("charset 数据库的字符集\n");
    printf("logfile 本程序运行的日志文件\n");
    printf("程序每10秒运行一次，由procctl调度。\n\n\n");  
    //分钟观测数据很重要，业务需求是越快越好，设置为5s，10s很合适，更多的就不合适了

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

  //进程的心跳，30秒足够
  //PActive.AddPInfo(30,"obtmindtodb");
  //注意，在调试程序时，可以启用类似以下的代码，将心跳时间设置得长一些，防止超时
  PActive.AddPInfo(5000,"obtmindtodb");


  //业务处理主函数
  _obtmindtodb(argv[1],argv[2],argv[3]);
  

  return 0;
}




void EXIT(int sig)
{
  logfile.Write("程序退出，sig=%d\n\n",sig);

  //增加断开数据库连接的代码
  conn.disconnect();

  exit(0);
}


//业务处理主函数
bool _obtmindtodb(char *pathname,char *connstr,char *charset)
{
  //创建目录对象
  CDir Dir;
  
  //打开目录，只需要xml文件
  if (Dir.OpenDir(pathname,"*.xml")==false)
  {
    //打开目录失败，写日志，返回false
    logfile.Write("Dir.OpenDir(%s) failed.\n",pathname); return false;
  }


  //创建CFile对象
  CFile File;  

  //创建对象
  CZHOBTMIND ZHOBTMIND(&conn,&logfile);
 
  //定义两个变量，记录文件的总记录数和成功插入数
  int totalcount=0;     //文件的总记录数
  int insertcount=0;    //成功插入记录数


  //定义一个计时器，记录每个文件的处理耗时
  CTimer Timer;

 
  while(true)
  {
    //读取目录，得到一个数据文件名
    if (Dir.ReadDir()==false) break;

    //将连接数据库的代码放在这里，只有在有数据处理时，才连接数据库，没有数据处理就不连数据库了
    //连接数据库
    //判断连接状态，如果已经连接了，就不连了connection类中，m_state这个成员记录了数据库的连接状态，0-未连接，1-已连接
    if (conn.m_state==0)
    {
      if (conn.connecttodb(connstr,charset)!=0)
      {
        //连接失败写日志
        logfile.Write("connect database(%s) failed.\n%s\n",connstr,conn.m_cda.message); return -1;
      }

      //连接成功给提示
      logfile.Write("connect database(%s) ok.\n",connstr);
    }

    //每一个新处理的文件totalcount和insertcount不同，所以在处理一个新的文件之前要清零这两个参数
    totalcount=insertcount=0;

    //只读的方式打开文件
    if (File.Open(Dir.m_FullFileName,"r")==false)
    {
       //打开文件失败，写日志，返回false
       logfile.Write("File.Open(%s) failed.\n",Dir.m_FullFileName); return false;
    }

    //声明一个buffer，存放从文件中读取的一行
    char strBuffer[1001];

    while(true)
    {
      //从文件中读取一行放在strBuffer中，以<endl/>为结尾
      if (File.FFGETS(strBuffer,1000,"<endl/>")==false) break;
    
      //读取到的内容写入日志，便于调试
      //logfile.Write("strBuffer=%s",strBuffer);
      

      //处理文件中的每一行
      totalcount++;

      //解析文件的每一行，将各个参数存入到结构体中
      ZHOBTMIND.SplitBuffer(strBuffer);

      //将结构体中的数据插入到数据库表中
      if (ZHOBTMIND.InsertTable()==true) insertcount++;    //插入成功的话，插入记录数+1
    }
    
    //删除文件、提交事务
    //File.CloseAndRemove();
    
    conn.commit();
  
    //记录每个文件的处理情况
    logfile.Write("已处理文件%s (totalcount=%d,insertcount=%d),耗时%.2f秒\n",Dir.m_FullFileName,totalcount,insertcount,Timer.Elapsed());
  }

  return true;
}
