
/*
 * 程序名：crtsurfdata1.cpp 本程序用于生成全国气象站点观测的分钟数据
 * 作者：卢卢
*/

#include "_public.h"

//日志类一般会定义为全局变量
CLogFile logfile(10);

int main(int argc, char *argv[])
{
   // inifile 全国气象站点参数文件
   // outpath 生成的测试数据存放目录
   // logfile 程序运行的日志，后台服务程序，一定要写日志
   // 该程序一共需要上述3个参数，所以argc的值为4
   if (argc!=4)
   {
    // 程序运行的方法不正确，提示正确方法
     printf("Using:./crtsurfdata1 infile outpath logfile\n");
     printf("Example:/project/idc2/bin/crtsurfdata1 /project/idc2/ini/stcode.ini /tmp/surfdata /log/idc/crtsurfdata1.log \n\n");
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

   logfile.Write("crtsurfdata1 开始运行 \n");

   //这里插入业务代码
  

   logfile.WriteEx("crtsurfdata1 运行结束 \n");

   return 0;
}

