/*
 *   obtmindtodb.cpp 本程序用于把全国站点分钟观测数据保存到数据库T_ZHOBTMIND表中，支持xml和csv两种文件格式
 *
*/

#include "_public.h"
#include "_mysql.h"
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


  /*
  //连接数据库，连接数据库的代码要放在加载参数文件之后，如果加载参数文件失败，都不用去连接数据库了
  if (conn.connecttodb(argv[2],argv[3])!=0)
  {
    //连接失败写日志
    logfile.Write("connect database(%s) failed.\n%s\n",argv[2],conn.m_cda.message); return -1;
  }

  //连接成功给提示
  logfile.Write("connect database(%s) ok.\n",argv[2]);


  //提交事务
  conn.commit();
  */

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


  while(true)
  {
    //读取目录，得到一个数据文件名
    if (Dir.ReadDir()==false) break;
 
    //显示出读取到的文件名
    logfile.Write("filename=%s\n",Dir.m_FullFileName);


    //打开文件
    /*
    while(true)
    {
      //处理文件中的每一行

    }
    */
    //删除文件、提交事务

  }

 


  return true;
}
