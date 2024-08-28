/*
 *   obtmindtodb.cpp 本程序用于把全国站点分钟观测数据保存到数据库T_ZHOBTMIND表中，支持xml和csv两种文件格式
 *
*/

#include "_public.h"
#include "_mysql.h"
#include "signal.h"


//存放数据的结构体，和数据库中对应的字段一致
//只要不做计算，一般都用字符串来存放整数和浮点数
struct st_zhobtmind
{
  char obtid[11];          //站点代码
  char ddatetime[21];      //数据时间，精确到分钟
  char t[11];              //温度，单位：0.1摄氏度
  char p[11];              //气压，单位：0.1百帕
  char u[11];              //相对湿度，0-100之间的值
  char wd[11];             //风向，0-360之间的值
  char wf[11];             //风速，单位0.1m/s
  char r[11];              //降雨量，0.1mm
  char vis[11];            //能见度，0.1米
};



//定义一个类，把拆分文件等复杂的函数放在类里面去
class CZHOBTMIND
{
public:
  connection    *m_conn;     //数据库连接
  CLogFile      *m_logfile;  //日志

  sqlstatement   m_stmt;     //插入表操作的sql

  char   m_buffer[1024];     //从文件中读到的一行
  struct st_zhobtmind   m_zhobtmind;  //全国站点分钟观测数据结构

  CZHOBTMIND();
  CZHOBTMIND(connection *conn,CLogFile *logfile);
  
 ~CZHOBTMIND();

  //把connection和CLogFile传进类里面去，用构造函数传也是可以的
  void BindConnLog(connection *conn,CLogFile *logfile);
  
  //把从文件中读取到的一行数据拆分到m_zhobtmind结构体中
  bool SplitBuffer(char *strBuffer);

  //把m_zhobtmind结构体中的内容插入到T_ZHOBTMIND表中
  bool InsertTable();
};


//构造函数中进行初始化的相关操作
CZHOBTMIND::CZHOBTMIND()
{
  m_conn=0; m_logfile=0;
}


CZHOBTMIND::CZHOBTMIND(connection *conn,CLogFile *logfile)
{
  m_conn=conn;
  m_logfile=logfile;
}


//析构函数，什么都不用做
CZHOBTMIND::~CZHOBTMIND()
{
}

//和有参数的构造函数功能一样，初始化参数
void CZHOBTMIND::BindConnLog(connection *conn,CLogFile *logfile)
{
  m_conn=conn;
  m_logfile=logfile;
}


//把从文件中读取到的一行数据拆分到m_zhobtmind结构体中
bool CZHOBTMIND::SplitBuffer(char *strBuffer)
{
  //初始化结构体
  memset(&m_zhobtmind,0,sizeof(struct st_zhobtmind));
      
  //把strBuffer的内容解析到结构体中
  GetXMLBuffer(strBuffer,"obtid",m_zhobtmind.obtid,10);
  GetXMLBuffer(strBuffer,"ddatetime",m_zhobtmind.ddatetime,14);
      

  //浮点数进行一下单位转换，把浮点数转换为整数
  //直接获取到的值存到临时变量中tmp
  char tmp[11];
  GetXMLBuffer(strBuffer,"t",tmp,10); 
  //临时变量不为空，表示有数据，转换一下再存到结构体中，先将取到的字符串转化为浮点数，*10之后，强转为整数
  if(strlen(tmp)>0)  snprintf(m_zhobtmind.t,10,"%d",(int)(atof(tmp)*10));
      
  GetXMLBuffer(strBuffer,"p",tmp,10);
  if(strlen(tmp)>0)  snprintf(m_zhobtmind.p,10,"%d",(int)(atof(tmp)*10));
     
  //u和wd本来就是整数，不用转换 
  GetXMLBuffer(strBuffer,"u",m_zhobtmind.u,10);
  GetXMLBuffer(strBuffer,"wd",m_zhobtmind.wd,10);

  GetXMLBuffer(strBuffer,"wf",tmp,10);
  if(strlen(tmp)>0)  snprintf(m_zhobtmind.wf,10,"%d",(int)(atof(tmp)*10));

  GetXMLBuffer(strBuffer,"r",tmp,10);
  if(strlen(tmp)>0)  snprintf(m_zhobtmind.r,10,"%d",(int)(atof(tmp)*10));

  GetXMLBuffer(strBuffer,"vis",tmp,10);
  if(strlen(tmp)>0)  snprintf(m_zhobtmind.vis,10,"%d",(int)(atof(tmp)*10));
  

  STRCPY(m_buffer,sizeof(strBuffer),strBuffer);
 
  return true;
}


//把结构体m_zhobtmind中的内容插入到T_ZHOBTMIND表中
bool CZHOBTMIND::InsertTable()
{
  //stmt对象绑定参数，要判断一下，只有没绑定才需要绑定，已经绑定了就不需要绑定
  //stmt中的成员函数m_state记录与数据库连接的绑定状态，0-未绑定，1-已绑定
  if (m_stmt.m_state==0)
  {
    m_stmt.connect(m_conn);
    m_stmt.prepare("insert into T_ZHOBTMIND(obtid,ddatetime,t,p,u,wd,wf,r,vis) values (:1,str_to_date(:2,'%%Y%%m%%d%%H%%i%%s'),:3,:4,:5,:6,:7,:8,:9)");
 
    //绑定参数
    m_stmt.bindin(1,m_zhobtmind.obtid,10);
    m_stmt.bindin(2,m_zhobtmind.ddatetime,14);
    m_stmt.bindin(3,m_zhobtmind.t,10);
    m_stmt.bindin(4,m_zhobtmind.p,10);
    m_stmt.bindin(5,m_zhobtmind.u,10);
    m_stmt.bindin(6,m_zhobtmind.wd,10);
    m_stmt.bindin(7,m_zhobtmind.wf,10);
    m_stmt.bindin(8,m_zhobtmind.r,10);
    m_stmt.bindin(9,m_zhobtmind.vis,10);
 }

  //把结构体中的数据插入表中,执行sql语句
  if (m_stmt.execute()!=0)
  {
    //1、执行失败的原因有哪些，是否全部的失败都要写日志
    //答：失败的原因主要有（1）记录重复（2）数据内容非法
    //2、如果失败了怎么办，程序是否需要继续？是否rollback()?是否返回false？
    //答：如果失败的原因是数据内容非法，记录日志后继续，如果是记录重复，不必记录日志，且继续
    if (m_stmt.m_cda.rc!=1062)
    {
      //非法内容记录日志
      m_logfile->Write("strBuffer=%s\n",m_buffer);
      m_logfile->Write("m_stmt.execute() failed.\n%s\n%s\n",m_stmt.m_sql,m_stmt.m_cda.message);
    }
    
    return false;
  }

  return true;
}




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
