
/*
 * 程序名：crtsurfdata3.cpp 本程序用于生成全国气象站点观测的分钟数据
 * 作者：卢卢
*/

#include "_public.h"

//全国气象站点参数结构体
struct st_stcode
{
   char provname[31];  //海拔高度
   char obtid[11];     //站号
   char obtname[31];   //站名
   double lat;         //纬度
   double lon;         //经度
   double height;      //海拔高度
}; 


//存放站点参数的容器
vector<struct st_stcode> vstcode;

//把站点参数文件中的数据加载到vSTcode容器中
bool LoadSTCode(const char *inifile);

//全国气象站点分钟观测数据结构
struct st_surfdata
{
   char obtid[11];     //站点代码
   char ddatetime[21]; //数据时间，格式为yyyymmddhh24miss
   int  t;             //气温：0.1摄氏度
   int  p;             //气压，0.1百帕
   int  u;             //相对湿度，0-100之间的值
   int  wd;            //风向，0-360之间的值
   int  wf;            //风速，0.1m/s
   int  r;             //降雨量0.1,mm
   int  vis;           //能见度0.1m
};


//存放观测数据的容器
vector<struct st_surfdata> vsurfdata;

//生成气象观测数据
void CrtSurfData();





//日志类一般会定义为全局变量
CLogFile logfile;

int main(int argc, char *argv[])
{
   // inifile 全国气象站点参数文件
   // outpath 生成的测试数据存放目录
   // logfile 程序运行的日志，后台服务程序，一定要写日志
   // 该程序一共需要上述3个参数，所以argc的值为4
   if (argc!=4)
   {
    // 程序运行的方法不正确，提示正确方法
     printf("Using:./crtsurfdata3 infile outpath logfile\n");
     printf("Example:/project/idc2/bin/crtsurfdata3 /project/idc2/ini/stcode.ini /tmp/surfdata /log/idc/crtsurfdata3.log \n\n");
     //详细说明程序的每个参数
     printf("infile 全国气象站点参数文件名。\n");
     printf("outpath 全国气象站点数据文件存放的目录。\n");
     printf("logfile 本程序运行的日志文件名。\n\n");

     return -1;
   }
   if(logfile.Open(argv[3],"a+",false)==false)
   {
     printf("logfile.Open(%s) failed.\n",argv[3]);
     return -1;
   }

   logfile.Write("crtsurfdata3 开始运行 \n");

   //这里插入业务代码
   //把站点参数文件中的数据加载到vSTcode容器中
   if (LoadSTCode(argv[1])==false) return -1;

   //模拟生成全国气象站点分钟观测数据，存放在vsurfdata容器中
   CrtSurfData();

   logfile.WriteEx("crtsurfdata3 运行结束 \n");

   return 0;
}


//把站点参数文件中的数据加载到vSTcode容器中
   bool LoadSTCode(const char *inifile)
{
  //定义CFilde对象，进行文件操作
  CFile File;


  //打开站点参数文件
  if(File.Open(inifile,"r")==false)
  {
  //打开失败，写入一条日志信息
  logfile.Write("File.Open(%s) failed.\n",inifile);return false;
  }  
  
  //定义一个buffer，存储从文件中读取的每一行
  char strBuffer[301];
  
   
  //定义CCmd类拆分字符串
  CCmdStr CmdStr;
  

  //定义结构体存放拆分后的结果
  struct st_stcode stcode;
   


  while(true)
  {
   //从站点参数文件中读取一行，如果已读取完，跳出循环
   if (File.Fgets(strBuffer,300,true)==false) break;
   
   
   //把读取到的一行拆分,第三个参数是删除空格
   CmdStr.SplitToCmd(strBuffer,",",true);
   
   //第一行不是有效数据，需要删掉，删除无效数据
   if(CmdStr.CmdCount()!=6) continue;
  

   //把站点参数的每个数据项保存到站点参数结构体中   
   //拆分，字符串指定长度，防止出现内存越界
   CmdStr.GetValue(0, stcode.provname,30); //省
   CmdStr.GetValue(1, stcode.obtid,10);    //站号
   CmdStr.GetValue(2, stcode.obtname,30);  //站名
   CmdStr.GetValue(3,&stcode.lat);         //纬度
   CmdStr.GetValue(4,&stcode.lon);         //经度
   CmdStr.GetValue(5,&stcode.height);      //海拔高度
   


   //把站点参数结构体放入站点参数容器
   vstcode.push_back(stcode);

  }

   //关闭文件（可以不需要，析构函数中会关闭）


  
   return true;
} 


//模拟生成全国气象站点分钟观测数据，存放在vsurfdata容器中
void CrtSurfData()
{
   //播随机种子
   srand(time(0));

   //获取当前时间，当做观测时间
   char strddatetime[21];
   
   //初始化字符串
   memset(strddatetime,0,sizeof(strddatetime));
   
   //获取当前时间
   LocalTime(strddatetime,"yyyymmddhh24miss");

   //结构体变量
   struct st_surfdata stsurfdata;


   //遍历气象站点参数的vscode容器
   for (int ii=0; ii<vstcode.size();ii++)
   {
     //初始化结构体变量
     memset(&stsurfdata,0,sizeof(struct st_surfdata));
     
     //用随机数填充分钟观测数据的结构体
     strncpy(stsurfdata.obtid,vstcode[ii].obtid,10);  //站点代码
     strncpy(stsurfdata.ddatetime,strddatetime,14);   //数据时间
     stsurfdata.t=rand()%351;                         //气温0-350 
     stsurfdata.p=rand()%265+10000;                   //气压10000-10264
          
     stsurfdata.u=rand()%100+1;                       //相对湿度
     stsurfdata.wd=rand()%360;                        //风向 
     stsurfdata.wf=rand()%150;                        //风素
     stsurfdata.r=rand()%16;                          //降雨量
     stsurfdata.vis=rand()%5001+100000;               //能见度

     
     //把观测数据的结构体放入vsurfdata容器
     vsurfdata.push_back(stsurfdata);
     
   }
   // printf("aaa\n");
}

