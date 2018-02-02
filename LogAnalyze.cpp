#include <iostream>
#include <string>
#include <string.h>
#include <set>
#include <time.h>
#include <vector>
#include <pthread.h>
#include <dirent.h>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include "base64.h"
#include <regex.h>
#include <limits.h>
#include "MysqlHelper.h"
#include <sstream>


using namespace mysqlhelper;
using namespace std;
using namespace __gnu_cxx;                  
#define CLOCKS_SEC ((clock_t)1000)
#define SIZE 1024
string path;
int THREAD_NUM;
int user_id_type;
int IS_RUN_APP,IS_RUN_WEB;
int IS_TEST_VERSION;
string configPath;

//hash_map全局变量,存储用户信息
hash_map<string,UserOperate> allUserInfo;//APP
hash_map<string,UserOperate> allUserInfoWeb;//Web

//存储所有URL与唯一标识ID信息（从DB中读取）
hash_map<string,TypeValue> indexMap;//APP
hash_map<string,TypeValue> indexMapWeb;//Web

vector<string> mainChVec;  //大渠道列表
vector<string> minorChVec; //小渠道列表

//存储块URL的ID
//vector<int> vBlock;

//中间变量，其个数根据线程数分配
vector< vector<string> > thVec;//APP
vector< vector<string> > thVecWeb;//Web

//注册人数,根据日志条数
int regNum;//APP
int regNumWeb;//Web

//实际注册人数（从prism_app中读取)
int regApp,regWeb;

string logDate;

pthread_mutex_t mutex;

hash_map<string,int> vipMap;//存储vip用户的手机号
map<string,string> m_mapConfigInfo;//配置文件信息
void ConfigFileRead(map<string, string>&);
string getNextDay(string input);
string getLastWeek(string input);
string getLastMonth(string input);
string getLastDay(string input);
bool runAnalyze(string todayStr);

/*********************
*  @brief 字符串截取
*  @para  str:待分割字符串
*  @para  Patten:分割形式
*********************/
std::vector<std::string> splitWithStl(const std::string &str,const std::string &pattern)
{
    std::vector<std::string> resVec;

    if ("" == str)
    {
        return resVec;
    }
    //方便截取最后一段数据
    std::string strs = str + pattern;

    size_t pos = strs.find(pattern);
    size_t size = strs.size();

    while (pos != std::string::npos)
    {
        std::string x = strs.substr(0,pos);
        resVec.push_back(x);
        strs = strs.substr(pos+1,size);
        pos = strs.find(pattern);
    }

    return resVec;
}

/*********************
*  @brief 显示用户信息
*  @para  info:用户信息
*********************/
void allUserInfoShow(hash_map<string,UserOperate> info)
{
    hash_map<string,UserOperate>::iterator iter;
    for(iter = info.begin(); iter != info.end(); iter++)
    {
        std::cout << "UserID:" << iter->first << "    " << "device:" << iter->second.device  << "    " << "tele: " << iter->second.teleNumber << "    " << "action: " << iter->second.actions.size()<< endl;
    }
}

/*********************
*  @brief 根据URL唯一标识判断其类别，并按类别分类
*  @para  action:URL唯一标识ID
*********************/
void insertToBigDataAccess(int action, hash_map<int,int>& mapAccess)
{
    hash_map<int,int>::iterator iter;
    iter = mapAccess.find(action);
    if(iter == mapAccess.end())//未找到
    {
        mapAccess[action] = 1;
    }
    else
    {
        mapAccess[action]++;
    }
}

/*********************
*  @brief 判断两个Action是否属于同一类别,不同类返回0，同类返回类别
*  @para  firAction:参数
*********************/
bool JudgeIsSort(int firAction, int secAction)
{
    int iThousFir = firAction/1000, iHundredFir = firAction/100%10;//千位与百位
    int iThousSec = secAction/1000, iHundredSec = secAction/100%10;//千位与百位
    if(iThousFir != iThousSec) return false;
    else if(iHundredFir != iHundredSec) return false;
    else return true;
}

int GetCategory(int action)
{
    int iThous = action/1000, iHundred = action/100%10;//千位与百位
    return iThous*1000 + iHundred*100;
}

/*********************
*  @brief 线程执行体：用户日志分析
*  @para  pPara:参数
*********************/
void* ThreadFunc(void *pPara)
{
    clock_t start,finish;
    start = clock();
    char buffer[SIZE]={0};
    string message;
    vector<string> tmpVec;
    int flag = *((int*)(&pPara));
    pthread_mutex_lock(&mutex);
    tmpVec = thVec[flag-1];
    pthread_mutex_unlock(&mutex);
    int countL = 0;
    int emptyId = 0;
    try{
        for(vector<string>::iterator it = tmpVec.begin(); it != tmpVec.end(); it++)
        {
            bool isDis = false;
            if((*it).find("api2") == string::npos)
            {
                isDis = true;
            }
            cout << *it << endl;
            //处理日志文件
            ifstream infile;
            const char* fileN = (path + *it).c_str();
    	    infile.open(fileN);
            if(!infile.is_open())
            {
                cout << "Unable to open file " << path << *it << endl;
                return (void*)0;
            }
            vector<string> splitStr;
            
            while(getline(infile,message))
            {
                countL++;
                emptyId = 0;
                //cout << message << endl;
                splitStr = splitWithStl(message,"\t");
                if(splitStr.size() < 3)
                {
                    cout << "error ---- " << endl;
                    continue;
                }
                if(splitStr.size() < 18)
                {
                    //cout << *it <<  " error ---- " << splitStr[1] << "----" << splitStr[2] << endl;
                    continue;
                }
                string mobile = "";
                if(splitStr[16] == "\"-\"" || splitStr[16] == "\"\"") continue;//除去非APP端
                if(!user_id_type)//统计注册用户
                {
                    if(splitStr[4].find("v3/user/reg") != string::npos) 
                    {
                        pthread_mutex_lock(&mutex);
                        regNum++;
                        pthread_mutex_unlock(&mutex);
                    }
                }
                if(!user_id_type && (splitStr[13] == "\"-\"" || splitStr[13] == "\"\"")) continue;//临时添加，去除无电话用户

                if(isDis)
                {
                    //如果此条记录不存在查看股权结构行为
                    if(splitStr[4].find("/dis/equityratio") == string::npos) continue;

                    if(user_id_type && (splitStr[17] == "\"-\"" || splitStr[17] == "\"\"")) continue; //临时添加（因股权结构日志中无deviceID)
                }
                else
                {
                    if(splitStr[5] == "\"401\"" || splitStr[5] == "\"403\"" || splitStr[5] == "\"501\"" || splitStr[5] == "\"503\"") continue;
                    
                    //去除deviceId为空的用户
                    if(user_id_type && (splitStr[17] == "\"-\"" || splitStr[17] == "\"\""))
                    {
                        emptyId++;
                        continue;
                    }
                }

                if(splitStr[13] != "\"-\"" && splitStr[13] != "\"\"")//解析用户手机号
                {
                    vector<string> encoded = splitWithStl(splitStr[13], ".");
                    //Android 版本过低的话该字段日志本身存储有问题，暂时pass掉
                    if(encoded.size() <= 2) 
                    {
                        //cout<< *it  << " userId error----"<< splitStr[1] << "---" << splitStr[13] << endl;
                        if(!user_id_type) continue;
                    }
                    else
                    {
                        mobile = base64_decode(encoded[1]);
                        if(mobile.length() < 12)
                        {
                            continue;
                        }
                        mobile = mobile.substr(8,11);
                    }
                }

                string userId;
                if(user_id_type) 
                {
                    userId = splitStr[17];
                }
                else
                {
                    userId = mobile;
                    if(mobile == "") cout << "TTTtt-------------------------------------------------" << endl;
                }
                hash_map<string,UserOperate>::iterator iter;
                iter = allUserInfo.find(userId);
                if(iter == allUserInfo.end()) // if not find --首次插入
                {
                    UserOperate ope;
                    //解析用户信息，并初次写入
                    ope.userId = userId;
                    ope.teleNumber = mobile;
                    ope.device = splitStr[16].substr(1,3) == "And" ? 1:0;
                    ope.ipAddress = splitStr[1];
                    ope.loginData = splitStr[3];
                    ope.channel = splitStr[15].substr(1,splitStr[15].length() - 2);
     
                    if(vipMap.find(mobile) != vipMap.end())
                    {
                        ope.isVip = true;
                    }
                    else
                    {
                        ope.isVip = false;
                    }
                    if(splitStr[15] == "\"-\"" || splitStr[15] == "\"\"")
                    {
                        ope.mainCh = "";
                        ope.minorCh = "";
                        ope.minorType = "";
                    }
                    else
                    {
                        map<string,string> channelMap;
                        channelMap = UserOperate::GetChannel(splitStr[15].substr(1,splitStr[15].length() - 2),mainChVec,minorChVec);
                        ope.mainCh = channelMap["MainChannel"];
                        ope.minorCh = channelMap["MinorChannel"];
                        ope.minorType = channelMap["MinorType"];
                    }
                    //提取用户行为
                    int action = UserOperate::JudgeMatch(splitStr[4],indexMap);
                    if(action != 0)
                    { 
                        //更新isAccess
                        Action act = {atof(splitStr[2].c_str()),splitStr[4],action};
                        ope.actions.push_back(act);
                        pthread_mutex_lock(&mutex);
                        allUserInfo[ope.userId] = ope;
                        pthread_mutex_unlock(&mutex);
                    }
                    else
                    {
                        pthread_mutex_lock(&mutex);
                        allUserInfo[ope.userId] = ope;
                        pthread_mutex_unlock(&mutex);
                    }
                }
                else
                {
                    //更新渠道信息
                    if(splitStr[15] != "\"-\"" && splitStr[15] != "\"\"")
                    {
                        string channel = splitStr[15].substr(1,splitStr[15].length() - 2);
                        map<string,string> channelMap;
                        channelMap = UserOperate::GetChannel(splitStr[15].substr(1,splitStr[15].length() - 2),mainChVec,minorChVec);
                        pthread_mutex_lock(&mutex);
                        iter->second.channel = channel;
                        iter->second.mainCh = channelMap["MainChannel"];
                        iter->second.minorCh = channelMap["MinorChannel"];
                        iter->second.minorType = channelMap["MinorType"];
                        pthread_mutex_unlock(&mutex);
                    }
                    //更新手机号信息
                    pthread_mutex_lock(&mutex);
                    if(iter->second.teleNumber == "" && mobile != "") iter->second.teleNumber = mobile;
                    pthread_mutex_unlock(&mutex);

                    int action = UserOperate::JudgeMatch(splitStr[4],indexMap);
                    if(action != 0)
                    {
                        Action act = {atof(splitStr[2].c_str()),splitStr[4],action};
                        pthread_mutex_lock(&mutex);
                        iter->second.actions.push_back(act);
    		            pthread_mutex_unlock(&mutex);
                    }
                }
            }
            stringstream stream;  
            stream<<countL;  
            string stemp=stream.str(); 
            cout << "num:  " << stemp << endl;
        }
    }catch(exception &e)
    {
        cout << "woowowowoowo" << endl;
    } 
    finish = clock();
    double duration = (double)(finish-start)/CLOCKS_SEC;
    cout << duration << " seconds." << endl; 
}
int tycid=0;
/*********************
*  @brief 线程执行体：用户日志分析
*  @para  pPara:参数
*********************/
void* ThreadFuncWeb(void *pPara)
{
    clock_t start,finish;
    start = clock();
    char buffer[SIZE]={0};
    string message;
    vector<string> tmpVec;
    int flag = *((int*)(&pPara));
    pthread_mutex_lock(&mutex);
    tmpVec = thVecWeb[flag-1];
    pthread_mutex_unlock(&mutex);
    int countL = 0;
    int emptyId = 0;
    int empios = 0,empzd = 0,emp54 = 0,emplsh;//临时变量
    try{ 
        for(vector<string>::iterator it = tmpVec.begin(); it != tmpVec.end(); it++)
        {
            bool isClk = false;
            if((*it).find("clk") != string::npos)
            {
                isClk = true;
                //cout << *it << endl;
            }
            cout << *it << endl;
            //处理日志文件
            ifstream infile;
            const char* fileN = (path + *it).c_str();
            infile.open(fileN);
            if(!infile.is_open())
            {
                cout << "Unable to open file " << path << *it << endl;
                return (void*)0;
            }
            vector<string> splitStr;
            
            while(getline(infile,message))
            {
                countL++;
                //cout << message << endl;
                splitStr = splitWithStl(message,"\t");
                string mobile = "",ipadd = "",login = "",url = "",timestamp = "";

                if(splitStr.size() < 3)
                {
                    cout << "error ---- " << endl;
                    continue;
                }
                if(isClk)
                {
                    //GET /pingd?srctype=getsret&ch=company.shareholder&sc=iosperson&mobi=13926965444&diviceid=3DE0E798-EAF4-452D-845A-38AAE30CF1AC&rand=1501549193000 HTTP/1.1
                    //cout << splitStr.size() << endl;
                    if(splitStr.size() < 10)
                    {
                        cout << *it <<  " error ---- " << splitStr[1] << "----" << splitStr[2] << endl;
                        continue;
                    }
                    //去除非web端日志和无手机号用户日志
                    if(splitStr[2].find("sc=person") == string::npos) //如果没找到
                    {
                        //cout << "aaa----" << splitStr[2] << endl;
                        continue;
                    }
                    if(splitStr[2].find("mobi=undefined") != string::npos)  //如果找到了，说明没有手机号
                    {
                        continue;
                    }
                    if(splitStr[2].find("mobi=&") != string::npos)  //如果找到了，说明没有手机号
                    {
                        continue;
                    }
                    if(splitStr[2].find("CompanySearch.Filter.Lianxidia") == string::npos && splitStr[2].find(" CompanySearch.Filter.Shangbiao") == string::npos
                        && splitStr[2].find("CompanySearch.Filter.Zhuanli") == string::npos && splitStr[2].find("CompanySearch.Filter.Shixin") == string::npos)
                    {
                        //cout << "bbb----" << splitStr[2] << endl;
                        continue;
                    } 
                    int index = splitStr[2].find("mobi=");
                    mobile = splitStr[2].substr(index+5,11);
                    //cout << mobile << endl;
                    ipadd = splitStr[1];
                    login = splitStr[0];
                    url = splitStr[2];
                    timestamp = splitStr[9];
                    //cout << ipadd << "--" << login << "--" << url << "--" << timestamp << endl;
                }
                else
                {
                    if(splitStr.size() < 19)
                    {
                        cout << *it <<  " error ---- " << splitStr[1] << "----" << splitStr[2] << endl;
                        continue;
                    }

                    if(splitStr[4].find("cd/reg.json") != string::npos) 
                    {
                        pthread_mutex_lock(&mutex);
                        regNumWeb++;
                        pthread_mutex_unlock(&mutex);
                    }

                    //if(splitStr[19] == "-" || splitStr[19] == "\"-\"" || splitStr[19] == "\"\"") continue;//临时添加，去除无deviceID用户

                    if(splitStr[5] == "\"401\"" || splitStr[5] == "\"403\"" || splitStr[5] == "\"501\"" || splitStr[5] == "\"503\"") continue;
                    if(splitStr[13] == "\"-\"" || splitStr[13] == "\"\"") continue;//临时添加，去除无电话用户,只统计登录用户行为

                    //string userId = splitStr[19];
                    if(splitStr[13] == "\"OK\"")
                    {
                        continue;
                    }
                    vector<string> encoded = splitWithStl(splitStr[13], ".");
                    //Android 版本过低的话该字段日志本身存储有问题，暂时pass掉
                    if(encoded.size() <= 2) 
                    {
                        //cout<< *it  << " userId error----"<< splitStr[1] << "---" << splitStr[2] << endl;
                        continue;
                    }
                    else
                    {
                        if(encoded[1] == "OK")
                        {
                            mobile = base64_decode(encoded[1]);
                            cout << "------" << mobile.length() << endl; 
                            continue;
                        }
                        mobile = base64_decode(encoded[1]);
                        if(mobile.length() < 12)
                        {
                            continue;
                        }
                        mobile = mobile.substr(8,11);//暂时截取手机号作为唯一标识
                        if(mobile.length() != 11)
                        {
                            continue;
                        }
                    }
                    ipadd = splitStr[1];
                    login = splitStr[3];
                    url = splitStr[4];
                    timestamp = splitStr[2];
                }

                string userId = mobile;
                hash_map<string,UserOperate>::iterator iter;
                iter = allUserInfoWeb.find(userId);
                if(iter == allUserInfoWeb.end()) // if not find --首次插入
                {
                    UserOperate ope;
                    //解析用户信息，并初次写入
                    ope.userId = userId;
                    ope.teleNumber = mobile;
                    ope.ipAddress = ipadd;//splitStr[1];
                    ope.loginData = login;//splitStr[3];
     
                    if(vipMap.find(mobile) != vipMap.end())
                    {
                        ope.isVip = true;
                    }
                    else
                    {
                        ope.isVip = false;
                    }

                    //提取用户行为
                    int action;
                    if(isClk) action = 6401;
                    else action = UserOperate::JudgeMatch(url,indexMapWeb);
                    if(action != 0)
                    { 
                        //更新isAccess
                        Action act = {atof(timestamp.c_str()),url,action};
                        ope.actions.push_back(act);
                        pthread_mutex_lock(&mutex);
                        allUserInfoWeb[ope.userId] = ope;
                        pthread_mutex_unlock(&mutex);
                    }
                    else
                    {
                        pthread_mutex_lock(&mutex);
                        allUserInfoWeb[ope.userId] = ope;
                        pthread_mutex_unlock(&mutex);
                    }
                }
                else
                {
                    //更新手机号信息
                    pthread_mutex_lock(&mutex);
                    if(iter->second.teleNumber == "" && mobile != "") iter->second.teleNumber = mobile;
                    pthread_mutex_unlock(&mutex);

                    int action;
                    if(isClk) action = 6401;
                    else action = UserOperate::JudgeMatch(url,indexMapWeb);

                    if(action != 0)
                    {
                        Action act = {atof(timestamp.c_str()),url,action};
                        pthread_mutex_lock(&mutex);
                        iter->second.actions.push_back(act);
                        pthread_mutex_unlock(&mutex);
                    }
                }
            }
            stringstream stream;  
            stream<<countL;  
            string stemp=stream.str(); 
            cout << "num:  " << stemp << endl;
        }
    }catch(exception &e)
    {
        cout << "woowowowoowo" << endl;
    } 
    finish = clock();
    double duration = (double)(finish-start)/CLOCKS_SEC;
    cout << duration << " seconds." << endl; 
}

/*********************
*  @brief 主函数
*********************/
int main(int argc, char** argv)
{
    if(argc == 1) cout << "please input configuration path!" << endl;
    configPath = argv[1];
    ConfigFileRead(m_mapConfigInfo);
    //path = m_mapConfigInfo["path"];
    THREAD_NUM = atoi(m_mapConfigInfo["threadNum"].c_str());
    user_id_type = atoi(m_mapConfigInfo["USER_ID_TYPE"].c_str());
    IS_TEST_VERSION = atoi(m_mapConfigInfo["IS_TEST_VERSION"].c_str());

    IS_RUN_APP = atoi(m_mapConfigInfo["IS_RUN_APP"].c_str());
    IS_RUN_WEB = atoi(m_mapConfigInfo["IS_RUN_WEB"].c_str());
    regNum = 0;
    regApp = 0;
    regWeb = 0;

    string endDate;
    if(argc != 2)
    {
        std::cout << "Please input start date info(Eq:20170518):";
        cin >> logDate;

        std::cout << "Please input end date info(Eq:20170706):";
        cin >> endDate;

        while(logDate != getNextDay(endDate))
        {
            cout << "----" << logDate << endl;
            path = m_mapConfigInfo["path"] + logDate + "/";
            //string todayStr = logDate.substr(0,4) + "-" + logDate.substr(4,2) + "-" + logDate.substr(6,2);
            //cout << todayStr << endl;
            if(!runAnalyze(logDate))
            {
                cout << "runAnalyze error!";
                return 0;
            }
            logDate = getNextDay(logDate);
        }
    }
    else
    {
        if(atoi(m_mapConfigInfo["isManualInput"].c_str()))
        {
            std::cout << "Please input date info(Eq:20170518):";
            cin >> logDate;
        }
        else
        {
            time_t t = time(0);
            char ch[64];
            strftime(ch, sizeof(ch), "%Y-%m-%d", localtime(&t));
            logDate = getLastDay(ch);
            std::cout << "Today is: " << logDate << std::endl; 
        }

        path = m_mapConfigInfo["path"] + logDate + "/";
        //string todayStr = logDate.substr(0,4) + "-" + logDate.substr(4,2) + "-" + logDate.substr(6,2);
        runAnalyze(logDate);
    }
    return 0;
}

bool runAnalyze(string todayStr)
{
    allUserInfo.clear();
    allUserInfoWeb.clear();
    regNum = 0;
    regNumWeb = 0;

    clock_t start,finish;
    double duration;
    start = clock();
    tm tm_;  
    time_t t_;  
    char buf[128]= {0};  
    
    string nextDay = getNextDay(todayStr);
    std::cout << nextDay << std::endl;
    todayStr = todayStr.substr(0,4) + "-" + todayStr.substr(4,2) + "-" + todayStr.substr(6,2);
    nextDay = nextDay.substr(0,4) + "-" + nextDay.substr(4,2) + "-" + nextDay.substr(6,2);

    //读取vip列表:之前的所有VIP
    MysqlHelper msHelperDB;
    msHelperDB.init(m_mapConfigInfo["ip"],m_mapConfigInfo["user"],m_mapConfigInfo["password"],m_mapConfigInfo["database"],"utf8",3306,0);
    //msHelperDB.init("rm-2zeo8i398c85fxu12.mysql.rds.aliyuncs.com","jindi","J1ndiApp","prism_app","utf8",3306,0);
    msHelperDB.connect();

    string sSqlDB = "select mobile from prism_app.user where state >=4 and vip_from_time BETWEEN '2016-01-01' AND '" + todayStr + "'  and vip_to_time > '" + todayStr + "'";
    //select mobile from prism_app.user where state >=3 and vip_from_time BETWEEN '2016-01-01' AND '" + todayStr + "'";
    vipMap = msHelperDB.queryRecordVIP(sSqlDB);
    cout << "vipNumber :" << vipMap.size() << endl;

    int actPayNum = 0,actPayNumWeb = 0;//当日实际支付人数
    int buyVip = 0, buyVipWeb = 0;//今日购买VIP人数
    int todayToVip = 0,todayToVipWeb = 0;//今日注册并购买VIP人数
    int lastWeekToVip = 0,lastWeekToVipWeb = 0;//上一周内注册并购买VIP人数
    int lastMonthToVip = 0,lastMonthToVipWeb = 0;//上一个月内注册并购买VIP人数
    int real_reg_app = 0,real_reg_web = 0,total_user = 0,vip_user = 0, vip1_user = 0, vip2_user = 0, vip3_user = 0,actual_pay_app = 0,actual_pay_web = 0;//总量统计
    string black_list = "'667','953','2768','786033','1020134','1020261','3528598','4233003','4233263','4233345','4233375','4233407','4237925',\
        '4238184','4372291','4375457','4375524','4664123','4664132','4664150','4760417','4760568','4760569','4760570','4760571',\
        '4761747','4762217','4762218','4762219','4762220','4762221','4762222','4762223','4762224','4762225','4762226','4762227',\
        '4762228','4762229','4762230','4762231','4762232','4762313','4762314','4762315','4762316','4762317','4762318','4762319',\
        '4762320','4762321','4762322','4762323','4762324','5828491','5851230'";
    if(IS_RUN_APP)
    {
        sSqlDB = "select count(DISTINCT user_id) from prism_app.order_info where update_date like '" + todayStr + "%' AND  (order_id like 'A%' or order_id like 'I%') AND actual_amount != 0 and STATUS = 1 and user_id not in (" + black_list + ")";
        cout << sSqlDB << endl;
        actPayNum = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where app_id is not null and create_time like '" + todayStr + "%'";
        cout << sSqlDB << endl;
        regApp = msHelperDB.queryRecordPayNum(sSqlDB);

        //购买vip总人数
        sSqlDB = "select count(order_id) from prism_app.order_info where (type = 50 or type = 60 or type = 70) AND (order_id like 'A%' or order_id like 'I%') and status = 1 AND date_format(update_date,'%Y-%m-%d') = '" + todayStr + "' and user_id not in (" + black_list + ")";
        buyVip = msHelperDB.queryRecordPayNum(sSqlDB);
        cout << sSqlDB << endl;

        //今日注册今日购买人数
        sSqlDB = "SELECT count(*) from prism_app.user where user.id in (select DISTINCT user_id from prism_app.order_info where (type = 50 or type = 60 or type = 70) AND  (order_id like 'A%' or order_id like 'I%') AND actual_amount != 0 and status = 1 and user_id not in (" + black_list + ")) and vip_from_time like '" + todayStr + "%' and create_time like '" + todayStr + "%'";
        todayToVip = msHelperDB.queryRecordPayNum(sSqlDB);
        cout << sSqlDB << endl;

        //一周内注册今日购买VIP人数
        sSqlDB = "SELECT count(*) from prism_app.user where user.id in (select DISTINCT user_id from prism_app.order_info where (type = 50 or type = 60 or type = 70) AND  (order_id like 'A%' or order_id like 'I%') AND actual_amount != 0 and status = 1 and user_id not in (" + black_list + ")) and vip_from_time like '" + todayStr + "%' and create_time between '" + getLastWeek(todayStr) + "' and '" + todayStr + "'";
        lastWeekToVip = msHelperDB.queryRecordPayNum(sSqlDB);
        cout << sSqlDB << endl;

        //一个月内注册今日购买VIP人数
        sSqlDB = "SELECT count(*) from prism_app.user where user.id in (select DISTINCT user_id from prism_app.order_info where (type = 50 or type = 60 or type = 70) AND  (order_id like 'A%' or order_id like 'I%') AND actual_amount != 0 and status = 1 and user_id not in (" + black_list + ")) and vip_from_time like '" + todayStr + "%' and create_time between '" + getLastMonth(todayStr) + "' and '" + getLastWeek(todayStr) + "'";
        lastMonthToVip = msHelperDB.queryRecordPayNum(sSqlDB);
        cout << sSqlDB << endl;

        sSqlDB = "SELECT count(*) from prism_app.user where create_time like '" + todayStr + "%' and app_id is not null and cdpassword is not null";
        real_reg_app = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where create_time <= '" + todayStr + "'";
        total_user = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where state >= 4 and create_time <= '" + todayStr + "'";
        vip_user = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where state = 5 and create_time <= '" + todayStr + "'";
        vip1_user = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where state = 6 and create_time <= '" + todayStr + "'";
        vip2_user = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where state = 7 and create_time <= '" + todayStr + "'";
        vip3_user = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT COUNT(DISTINCT user_id) FROM order_info WHERE (type = 11 or type = 14 or type = 17 or type = 20) and (order_id like 'A%' or order_id like 'I%') AND actual_amount >0 and deleted = 0 AND status=1 AND date_format(update_date,'%Y-%m-%d') = '" + todayStr + "' and user_id not in (" + black_list + ")";
        actual_pay_app = msHelperDB.queryRecordPayNum(sSqlDB);
        cout << "actual_pay_app:" << actual_pay_app << endl;
    }
    if(IS_RUN_WEB)
    {
        sSqlDB = "select count(DISTINCT user_id) from prism_app.order_info where update_date like '" + todayStr + "%' AND  (order_id like 'W%') AND actual_amount != 0 and STATUS = 1 and user_id not in (" + black_list + ")";
        cout << sSqlDB << endl;
        actPayNumWeb = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where app_id is null and create_time like '" + todayStr + "%'";
        cout << sSqlDB << endl;
        regWeb = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "select count(order_id) from prism_app.order_info where (type = 50 or type = 60 or type = 70) AND (order_id like 'W%') and status = 1 AND date_format(update_date,'%Y-%m-%d') = '" + todayStr + "' and user_id not in (" + black_list + ")";
        buyVipWeb = msHelperDB.queryRecordPayNum(sSqlDB);
        cout << sSqlDB << endl;

        sSqlDB = "SELECT count(*) from prism_app.user where user.id in (select DISTINCT user_id from prism_app.order_info where (type = 50 or type = 60 or type = 70) AND  (order_id like 'W%') AND actual_amount != 0 and status = 1 and user_id not in (" + black_list + ")) and vip_from_time like '" + todayStr + "%' and create_time like '" + todayStr + "%'";
        todayToVipWeb = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where user.id in (select DISTINCT user_id from prism_app.order_info where (type = 50 or type = 60 or type = 70) AND  (order_id like 'W%') AND actual_amount != 0 and status = 1 and user_id not in (" + black_list + ")) and vip_from_time like '" + todayStr + "%' and create_time between '" + getLastWeek(todayStr) + "' and '" + todayStr + "'";
        lastWeekToVipWeb = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where user.id in (select DISTINCT user_id from prism_app.order_info where (type = 50 or type = 60 or type = 70) AND  (order_id like 'W%') AND actual_amount != 0 and status = 1 and user_id not in (" + black_list + ")) and vip_from_time like '" + todayStr + "%' and create_time between '" + getLastMonth(todayStr) + "' and '" + getLastWeek(todayStr) + "'";
        lastMonthToVipWeb = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT count(*) from prism_app.user where create_time like '" + todayStr + "%' and app_id is null and cdpassword is not null";
        real_reg_web = msHelperDB.queryRecordPayNum(sSqlDB);

        sSqlDB = "SELECT COUNT(DISTINCT user_id) FROM order_info WHERE (type = 11 or type = 14 or type = 17 or type = 20) and (order_id like 'W%') AND actual_amount >0 and deleted = 0 AND status=1 AND date_format(update_date,'%Y-%m-%d') = '" + todayStr + "' and user_id not in (" + black_list + ")";
        actual_pay_web = msHelperDB.queryRecordPayNum(sSqlDB);
        cout << "actual_pay_web:"<< actual_pay_web << endl;
    }
    msHelperDB.disconnect();

    //预处理
    vector<string> fileVec;//存储文件夹下所有的文件名--APP
    fileVec.clear();

    vector<string> fileVecWeb;//存储文件夹下所有的文件名--Web
    fileVecWeb.clear();

    cout << "Path:----" << path << endl;
    struct dirent * filename;
    DIR * dir;
    dir = opendir(path.c_str());
    if(NULL == dir)
    {
       cout << "Cannot open dir: " << path << endl;
       return 0;
    }
    cout << "Success opened the dir!" << path << endl;
    while( (filename = readdir(dir) ) != NULL)
    {
       if( strcmp( filename->d_name , "." ) == 0 ||   
            strcmp( filename->d_name , "..") == 0 )  
            continue;
       string tmp = filename->d_name;    
       //cout << "----" << tmp << endl;   
       //找到所有满足条件的日志
       if(IS_RUN_APP)
       {
           if(tmp.substr(0,4) == "api2" && tmp.find("error") == string::npos)
           {
               //cout<<filename->d_name <<endl; 
               fileVec.push_back(filename->d_name);
           }
           else if(tmp.substr(0,3) == "dis" && tmp.find("error") == string::npos)
           {
               //cout<<filename->d_name <<endl; 
               fileVec.push_back(filename->d_name);
           }
       }
       if(IS_RUN_WEB)
       {
            if(tmp.substr(0,10) == "tianyancha" && tmp.find("error") == string::npos && tmp.find("nowww") == string::npos)
            { 
                fileVecWeb.push_back(filename->d_name);
            }
            else if(tmp.substr(0,3) == "clk")
            {
                fileVecWeb.push_back(filename->d_name);
            }
       }
    }
    
    //初始化mysql对象并建立连接
    MysqlHelper mysqlHelper;
    mysqlHelper.init(m_mapConfigInfo["iptest"],m_mapConfigInfo["usertest"],m_mapConfigInfo["passwordtest"],m_mapConfigInfo["databasetest"],"utf8",3306,0);
    mysqlHelper.connect();

    //读取用户行为字典--APP
    string sSql;
    if(IS_RUN_APP)
    {
        sSql = "select netUrl,netId,searchType from NetDict where type = 0";
        indexMap = mysqlHelper.queryRecord(sSql);
    }
    if(IS_RUN_WEB)
    {
        sSql = "select netUrl,netId,searchType from NetDict where type = 1";
        indexMapWeb = mysqlHelper.queryRecord(sSql);
    }

    /*sSql = "select * from BlockDict";
    vBlock = mysqlHelper.queryRecordBlock(sSql);*/

    sSql = "select channelName from ChannelInfo where channelType >= 0 and channelType <= 11";
    mainChVec = mysqlHelper.queryRecordChannel(sSql);

    sSql = "select channelName from ChannelInfo where channelType >= 12";
    minorChVec = mysqlHelper.queryRecordChannel(sSql);

    mysqlHelper.disconnect();
    cout << "-----------" << endl; 

    thVec.clear();
    thVecWeb.clear();
    cout << "**************" << thVec.size() <<  "---" << fileVec.size() <<endl; 
    cout << "**************" << thVecWeb.size() <<  "---" << fileVecWeb.size() <<endl; 
    if(IS_RUN_APP)
    {
        int singleSize = fileVec.size()/THREAD_NUM + 1;
        //根据线程数进行任务分配
        for(int k = 0; k < THREAD_NUM; k++)
        {
            vector<string> tmpVec;
            for(int i = 0; i < singleSize; i++)
            {
                if((i*THREAD_NUM + k) >= fileVec.size()) break;
                tmpVec.push_back(fileVec[i*THREAD_NUM + k]);
            }
            thVec.push_back(tmpVec);
        }

        //create Threads
        pthread_t tid[THREAD_NUM];
        int code;
        for(int i = 0; i < THREAD_NUM; i++)
        {
            code = pthread_create(&tid[i],NULL,ThreadFunc,(void *)(i+1));
            if(code != 0)
            {
                cout << "Create New Thread Failed!" << endl;
            }
                cout << "New Thread Created!" << endl;
        }
        
        //主线程等待，待所有子线程执行完毕后继续处理数据
        void* thread_res;
        for(int i = 0; i < THREAD_NUM; i++)
        {
            pthread_join(tid[i],&thread_res);
        }
        cout << "Can continue work..." << endl;
    }
    if(IS_RUN_WEB)
    {
        //根据线程数进行任务分配
        for(int k = 0; k < THREAD_NUM; k++)
        {
            vector<string> tmpVec;
            int tmpSize = (k+1)*(fileVecWeb.size()/THREAD_NUM);
            if(k == THREAD_NUM - 1)
            {
                tmpSize = fileVecWeb.size();
            }
            for(int i = k*(fileVecWeb.size()/THREAD_NUM); i < tmpSize; i++)
            {
                tmpVec.push_back(fileVecWeb[i]);
            }
            thVecWeb.push_back(tmpVec);
        }

        //create Threads
        pthread_t tid[THREAD_NUM];
        int code;
        for(int i = 0; i < THREAD_NUM; i++)
        {
            code = pthread_create(&tid[i],NULL,ThreadFuncWeb,(void *)(i+1));
            if(code != 0)
            {
                cout << "Create New Thread Failed!" << endl;
            }
                cout << "New Thread Created!" << endl;
        }
        
        //主线程等待，待所有子线程执行完毕后继续处理数据
        void* thread_res;
        for(int i = 0; i < THREAD_NUM; i++)
        {
            pthread_join(tid[i],&thread_res);
        }
        cout << "Can continue work..." << endl;
    }

    //cout << "------------" << allUserInfoWeb.size() << endl;
    //return 0;
    
    finish = clock();
    duration = (double)(finish-start)/CLOCKS_PER_SEC;
    cout << duration << " seconds. Main Thread" << endl;
    start = clock();


    set<string>::iterator it;

    int res=0;
    try{
        mysqlHelper.connect();
        //创建表 （表名：UserInfo+日期）
        string tableName;
        if(IS_RUN_APP)
        {
            if(IS_TEST_VERSION) tableName = "UserInfoTest" + logDate;
            else tableName = "UserInfo" + logDate;
            mysqlHelper.createTable(tableName);
            cout << "create table succ" << endl;  
            int loginNum = 0,vipNum = 0;   
            try{  
                //插入用户信息表
                if(user_id_type)
                {
                    mysqlHelper.insertRecord(tableName,allUserInfo,loginNum,vipNum,regNum); 
                }
                else
                {
                    int tmp;
                    mysqlHelper.insertRecord(tableName,allUserInfo,loginNum,vipNum,tmp); 
                }
            }catch(exception &e)
            {
                cout << "insertRecord App has error" << endl;
            } 
            cout << "regNum:" << regNum << endl;

            //创建表
            if(IS_TEST_VERSION) tableName = "TotalStatisticsTest";
            else tableName = "TotalStatistics";
            mysqlHelper.createTable(tableName);
            vector<int> numVec;
            numVec.push_back(allUserInfo.size());
            numVec.push_back(loginNum);
            numVec.push_back(vipNum);
            numVec.push_back(actPayNum);
            numVec.push_back(regApp);
            numVec.push_back(todayToVip);
            numVec.push_back(lastWeekToVip);
            numVec.push_back(lastMonthToVip);
            numVec.push_back(buyVip);
            numVec.push_back(real_reg_app);
            numVec.push_back(total_user);
            numVec.push_back(vip_user);
            numVec.push_back(vip1_user);
            numVec.push_back(vip2_user);
            numVec.push_back(vip3_user);
            numVec.push_back(actual_pay_app);
            //res=mysqlHelper.insertRecordTotal(tableName, logDate, true, allUserInfo.size(),loginNum, vipNum,actPayNum,regApp,todayToVip,lastWeekToVip,lastMonthToVip,buyVip);
            res=mysqlHelper.insertRecordTotal(tableName, logDate, true, numVec);
            cout << "size: " << allUserInfo.size() << endl;
        }
        if(IS_RUN_WEB)
        {
            if(IS_TEST_VERSION) tableName = "WebUserInfoTest" + logDate;
            else tableName = "WebUserInfo" + logDate;
            mysqlHelper.createTable(tableName);
            cout << "create table succ" << endl;  
            int loginNum = 0,vipNum = 0;   
            try{
                //插入用户信息表
                //if(user_id_type)
                //{
                //   mysqlHelper.insertRecord(tableName,allUserInfoWeb,loginNum,vipNum,regNumWeb); 
                //}
                //else
                //{
                    int tmp;
                    mysqlHelper.insertRecord(tableName,allUserInfoWeb,loginNum,vipNum,tmp); 
                //}
            }catch(exception &e)
            {
                cout << "insertRecord Web has error" << endl;
            } 
            cout << "regNum:" << regNumWeb << endl;

            //创建表
            if(IS_TEST_VERSION) tableName = "TotalStatisticsTest";
            else tableName = "TotalStatistics";
            mysqlHelper.createTable(tableName);
            vector<int> numVec;
            numVec.push_back(allUserInfoWeb.size());
            numVec.push_back(loginNum);
            numVec.push_back(vipNum);
            numVec.push_back(actPayNumWeb);
            numVec.push_back(regWeb);
            numVec.push_back(todayToVipWeb);
            numVec.push_back(lastWeekToVipWeb);
            numVec.push_back(lastMonthToVipWeb);
            numVec.push_back(buyVipWeb);
            numVec.push_back(real_reg_web);
            numVec.push_back(0);
            numVec.push_back(0);
            numVec.push_back(0);
            numVec.push_back(0);
            numVec.push_back(0);
            numVec.push_back(actual_pay_web);
            //res=mysqlHelper.insertRecordTotal(tableName, logDate, false, allUserInfoWeb.size(),loginNum, vipNum,actPayNumWeb,regWeb,todayToVipWeb,lastWeekToVipWeb,lastMonthToVipWeb,buyVipWeb);
            res=mysqlHelper.insertRecordTotal(tableName, logDate, false, numVec);
            cout << "size: " << allUserInfoWeb.size() << endl;
        }
        mysqlHelper.disconnect();

    }catch(MysqlHelper_Exception& excep){
        cout<<excep.errorInfo;
        return -1;
    }
    cout<<"res:"<<res<<" insert successfully "<<endl;

    //allUserInfoShow(allUserInfo);
    finish = clock();
    duration = (double)(finish-start)/CLOCKS_PER_SEC;
    cout << duration << " seconds. Main Thread" << endl;
    /*time_t start,stop;
    start = time(NULL);
    foo();//dosomething
    stop = time(NULL);
    cout << "Use Time(s):%ld\n",(stop-start) << endl;*/
}

void ConfigFileRead( map<string, string>& m_mapConfigInfo )
{
    ifstream configFile;
    string path = configPath + "/setting.conf";
    cout << path << endl;
    configFile.open(path.c_str());
    string str_line;
    if (configFile.is_open())
    {
        while (!configFile.eof())
        {
            getline(configFile, str_line);
            if ( str_line.find('#') == 0 ) //过滤掉注释信息，即如果首个字符为#就过滤掉这一行
            {
                continue;
            }    
            size_t pos = str_line.find('=');
            string str_key = str_line.substr(0, pos);
            string str_value = str_line.substr(pos + 1);
            m_mapConfigInfo.insert(pair<string, string>(str_key, str_value));
        }
    }
    else
    {    
        cout << "Cannot open config file setting.config, path: ";
        exit(-1);
    }
}

string getNextDay(string input)
{
    tm tm_;  
    time_t t_;  
    char buf[128]= {0};  
    input = input.substr(0,4) + "-" + input.substr(4,2) + "-" + input.substr(6,2);
    string tmpStr = input + " 14:00:00";
    strcpy(buf, tmpStr.c_str());  
    strptime(buf, "%Y-%m-%d %H:%M:%S", &tm_); //将字符串转换为tm时间  
    tm_.tm_isdst = -1;  
    tm_.tm_mday = tm_.tm_mday + 1;
    t_  = mktime(&tm_); //将tm时间转换为秒时间   
      
    tm_ = *localtime(&t_);//输出时间  
    strftime(buf, 64, "%Y%m%d %H:%M:%S", &tm_);  
    string nextDay = buf;
    nextDay = nextDay.substr(0,8);
    std::cout << nextDay << std::endl;
    return nextDay;
}

string getLastDay(string input)
{
    tm tm_;  
    time_t t_;  
    char buf[128]= {0};  
    //input = input.substr(0,4) + "-" + input.substr(4,2) + "-" + input.substr(6,2);
    string tmpStr = input + " 14:00:00";
    strcpy(buf, tmpStr.c_str());  
    strptime(buf, "%Y-%m-%d %H:%M:%S", &tm_); //将字符串转换为tm时间  
    tm_.tm_isdst = -1;  
    tm_.tm_mday = tm_.tm_mday - 1;
    t_  = mktime(&tm_); //将tm时间转换为秒时间   
      
    tm_ = *localtime(&t_);//输出时间  
    strftime(buf, 64, "%Y%m%d %H:%M:%S", &tm_);  
    string nextDay = buf;
    nextDay = nextDay.substr(0,8);
    std::cout << nextDay << std::endl;
    return nextDay;
}

string getLastWeek(string input)
{
    tm tm_;  
    time_t t_;  
    char buf[128]= {0};  
    //input = input.substr(0,4) + "-" + input.substr(4,2) + "-" + input.substr(6,2);
    string tmpStr = input + " 14:00:00";
    strcpy(buf, tmpStr.c_str());  
    strptime(buf, "%Y-%m-%d %H:%M:%S", &tm_); //将字符串转换为tm时间  
    tm_.tm_isdst = -1;  
    tm_.tm_mday = tm_.tm_mday - 8;
    t_  = mktime(&tm_); //将tm时间转换为秒时间   
      
    tm_ = *localtime(&t_);//输出时间  
    strftime(buf, 64, "%Y-%m-%d %H:%M:%S", &tm_);  
    string nextDay = buf;
    nextDay = nextDay.substr(0,10);
    std::cout << nextDay << std::endl;
    return nextDay;
}

string getLastMonth(string input)
{
    tm tm_;  
    time_t t_;  
    char buf[128]= {0};  
    //input = input.substr(0,4) + "-" + input.substr(4,2) + "-" + input.substr(6,2);
    string tmpStr = input + " 14:00:00";
    strcpy(buf, tmpStr.c_str());  
    strptime(buf, "%Y-%m-%d %H:%M:%S", &tm_); //将字符串转换为tm时间  
    tm_.tm_isdst = -1;  
    tm_.tm_mday = tm_.tm_mday - 30;
    t_  = mktime(&tm_); //将tm时间转换为秒时间   
      
    tm_ = *localtime(&t_);//输出时间  
    strftime(buf, 64, "%Y-%m-%d %H:%M:%S", &tm_);  
    string nextDay = buf;
    nextDay = nextDay.substr(0,10);
    std::cout << nextDay << std::endl;
    return nextDay;
}