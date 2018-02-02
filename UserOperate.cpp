
#include "UserOperate.h"
#include <regex.h>
using namespace std;
UserOperate::UserOperate(void)
{
	
}

UserOperate::~UserOperate(void)
{
	
}
UserOperate::UserOperate(hash_map<string,TypeValue>& sMap, vector<int>& vBlock)
{
}

int UserOperate::JudgeMatch(const string& src,hash_map<string,TypeValue>& sMap)
{
   // cout << "***************** ssssssssss" << endl;
    hash_map<string,TypeValue>::iterator iter;
    //regex_t regex;
    for(iter = sMap.begin(); iter != sMap.end(); iter++)
    {
       
        if(iter->second.type == 0)
        {
            if(src.find(iter->first) != string::npos)
            {
               return iter->second.urlID;
            }
        }
        else
        {
            regex_t regex;
            string pattern = iter->first;
            int errcode =  regcomp(&regex,pattern.c_str(),REG_EXTENDED | REG_NOSUB);
            errcode = regexec(&regex,src.c_str(),0,NULL,0);
            if(errcode != REG_NOMATCH)
            {
                //cout << "**************** eeeeeeeeeeeee" << endl;
                regfree(&regex);    
                return iter->second.urlID;
            }
            regfree(&regex);
        }
    }
    return 0;
}

/*********************
*  @brief 渠道解析：解析出大渠道、小渠道、小渠道类别
*  @para  
*********************/
map<string,string> UserOperate::GetChannel(const string& src,vector<string> mainVec, vector<string> minorVec)
{
    map<string,string> tempMap;
    tempMap["MainChannel"] = "";
    tempMap["MinorChannel"] = "";
    tempMap["MinorType"] = "";
    //遍历大渠道列表，判断是否能在渠道信息里提取到大渠道
    for(vector<string>::iterator it = mainVec.begin(); it != mainVec.end(); ++it)
    {
        if(src.find(*it) != string::npos)
        {
            tempMap["MainChannel"] = *it;
            break;
        }
    }
    if(tempMap["MainChannel"] != "" && tempMap["MainChannel"] != "TuiGuang") return tempMap;

    //遍历小渠道类别列表，判断是否能在渠道信息里提取到小渠道
    for(vector<string>::iterator it = minorVec.begin(); it != minorVec.end(); ++it)
    {
        int index = -1;
        if((index = src.find(*it)) != string::npos)
        { 
            tempMap["MinorType"] = *it; //小渠道类别
            tempMap["MinorChannel"] = src.substr(index);
            break;
        }
    }

    return tempMap;
}
