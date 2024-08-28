/************************************************************************/
/*    程序名：idcapp.cpp，此程序是数据中心项目公用函数和类的实现文件    */
/*    作者：卢卢                                                        */
/************************************************************************/


#include "idcapp.h"

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
bool CZHOBTMIND::SplitBuffer(char *strBuffer,bool bisxml)
{
  //初始化结构体
  memset(&m_zhobtmind,0,sizeof(struct st_zhobtmind));
     

  if (bisxml==true)
  { 
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
  }
  else
  {
    //把strBuffer的内容解析到结构体中
    CCmdStr CmdStr;
    CmdStr.SplitToCmd(strBuffer,",");

    CmdStr.GetValue(0,m_zhobtmind.obtid,10);
    CmdStr.GetValue(1,m_zhobtmind.ddatetime,14);
    

    //浮点数进行一下单位转换，把浮点数转换为整数
    //直接获取到的值存到临时变量中tmp
    char tmp[11];
    CmdStr.GetValue(2,tmp,10); 
    //临时变量不为空，表示有数据，转换一下再存到结构体中，先将取到的字符串转化为浮点数，*10之后，强转为整数
    if(strlen(tmp)>0)  snprintf(m_zhobtmind.t,10,"%d",(int)(atof(tmp)*10));
    
    CmdStr.GetValue(3,tmp,10);
    if(strlen(tmp)>0)  snprintf(m_zhobtmind.p,10,"%d",(int)(atof(tmp)*10));
    
    //u和wd本来就是整数，不用转换 
    CmdStr.GetValue(4,m_zhobtmind.u,10);
    CmdStr.GetValue(5,m_zhobtmind.wd,10);

    CmdStr.GetValue(6,tmp,10);
    if(strlen(tmp)>0)  snprintf(m_zhobtmind.wf,10,"%d",(int)(atof(tmp)*10));

    CmdStr.GetValue(7,tmp,10);
    if(strlen(tmp)>0)  snprintf(m_zhobtmind.r,10,"%d",(int)(atof(tmp)*10));

    CmdStr.GetValue(8,tmp,10);
    if(strlen(tmp)>0)  snprintf(m_zhobtmind.vis,10,"%d",(int)(atof(tmp)*10));
  }


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

