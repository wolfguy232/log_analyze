#include <iostream>
#include <string>
#include <vector>
#include <regex.h>
#include <stdlib.h>
#include <map>

#include <ext/hash_map>
using namespace std;
using namespace __gnu_cxx;
namespace __gnu_cxx {
 template<> struct hash<std::string> {
     size_t operator()(const std::string& x) const {
         return hash<const char*>()(x.c_str());
     }
 };
}

struct  TypeValue
{
    int type;
    int urlID;
};

struct Action
{
   double timestamp;   //时间戳
   string url;         //URL地址
   int action;         //URL唯一标识ID
   //bool isVip;
   //用于将用户行为按时间戳排序
   bool operator <(const Action& right) const {
       return timestamp < right.timestamp;
   }
};

/*********************
*  @brief 用户信息类
*********************/
class UserOperate
{
    public:
	    UserOperate(void);
	    ~UserOperate(void);
      UserOperate(hash_map<string,TypeValue>& sMap, vector<int>& vBlock);
    public:
      string userId;     //用户ID
	    string teleNumber; //手机号码
	    bool isVip;        //VIP
	    int device;        //IOS:0 Andriod:1
	    string loginData;  //登录日期
      string ipAddress;  
      string channel; //渠道字符串
      string mainCh;  //大渠道
      string minorCh; //小渠道
      string minorType; //小渠道所属类别
	    vector<Action> actions;  //用户行为列表

   public:
      //判断用户所访问URL是否在监控列表中，如果存在，则返回URL唯一标识
      static int JudgeMatch(const string& src,hash_map<string,TypeValue>& sMap);
      static map<string,string> GetChannel(const string& src,vector<string> mainVec, vector<string> minorVec);
};
