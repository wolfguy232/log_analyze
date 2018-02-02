//mysqlhelper.cpp
#include "MysqlHelper.h"
#include <string.h>
#include <sstream>
#include <string>
#include <iostream>
#include <algorithm> 
#include <stdio.h>
#include <limits.h>
using namespace std;

namespace mysqlhelper
{
	MysqlHelper::MysqlHelper():_bConnected(false)
	{
	    _pstMql = mysql_init(NULL);
	}
	 
	MysqlHelper::MysqlHelper(const string& sHost, const string& sUser, const string& sPasswd, const string& sDatabase, 
		const string &sCharSet, int port, int iFlag):_bConnected(false)
	{
	    init(sHost, sUser, sPasswd, sDatabase, sCharSet, port, iFlag);
	 
	    _pstMql = mysql_init(NULL);
	}
	 
	MysqlHelper::MysqlHelper(const DBConf& tcDBConf):_bConnected(false)
	{
	    _dbConf = tcDBConf;
	 
	    _pstMql = mysql_init(NULL); 
	}
	 
	MysqlHelper::~MysqlHelper()
	{
	    if (_pstMql != NULL)
	    {
	        mysql_close(_pstMql);
	       _pstMql = NULL;
	    }
	}

	void MysqlHelper::init(const string& sHost, const string& sUser, const string& sPasswd, const string& sDatabase, const string &sCharSet, int port, int iFlag)
	{
		_dbConf._host = sHost;
		_dbConf._user = sUser;
		_dbConf._password = sPasswd;
		_dbConf._database = sDatabase;
		_dbConf._charset = sCharSet;
		_dbConf._port = port;
		_dbConf._flag = iFlag;
	}

	void MysqlHelper::init(const DBConf& tcDBConf)
	{
		_dbConf = tcDBConf;
	}

	void MysqlHelper::connect()
	{
		disconnect();
		 
		if( _pstMql == NULL)
		{
			_pstMql = mysql_init(NULL);
	    }
		 
		//建立连接后, 自动调用设置字符集语句
		if(!_dbConf._charset.empty()) {
			if (mysql_options(_pstMql, MYSQL_SET_CHARSET_NAME, _dbConf._charset.c_str())) 
			{
				throw MysqlHelper_Exception(string("MysqlHelper::connect: mysql_options MYSQL_SET_CHARSET_NAME ") + _dbConf._charset + ":" + string(mysql_error(_pstMql)));
			}
		} 
		 
		if (mysql_real_connect(_pstMql, _dbConf._host.c_str(), _dbConf._user.c_str(), _dbConf._password.c_str(), _dbConf._database.c_str(), _dbConf._port, NULL, _dbConf._flag) == NULL) 
		{
			throw MysqlHelper_Exception("[MysqlHelper::connect]: mysql_real_connect: " + string(mysql_error(_pstMql)));
		}
		cout << "Connet successful!" << endl;
		_bConnected = true;
	}
	 
	void MysqlHelper::disconnect()
	{
		if (_pstMql != NULL)
		{
			mysql_close(_pstMql);
			_pstMql = mysql_init(NULL);
		}
		cout << "Disconnect successful!" << endl;
		_bConnected = false; 
	}

	MYSQL *MysqlHelper::getMysql(void)
	{
		return _pstMql;
	}

	void MysqlHelper::execute(const string& sSql)
	{
		/**
		没有连上, 连接数据库
		*/
		if(!_bConnected)
		{
		    connect();
		}
		 
		_sLastSql = sSql;
		 
		int iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		if(iRet != 0)
		{
			/**
			自动重新连接
			*/
			int iErrno = mysql_errno(_pstMql);
			if (iErrno == 2013 || iErrno == 2006)
			{
                                cout << "reconncet...." << endl;
				connect();
				iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
			}
		}
		if (iRet != 0)
		{
                     cout << "Failed........" << endl;
			throw MysqlHelper_Exception("[MysqlHelper::execute]: mysql_query: [ " + sSql+" ] :" + string(mysql_error(_pstMql))); 
		}
	}

	MYSQL_RES * MysqlHelper::checkConnect(const string& sSql)
	{
		/**
		 没有连上, 连接数据库
		 */
		 if(!_bConnected)
		 {
		 	connect();
		 }
		 
		 _sLastSql = sSql;
		 
		 int iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		 if(iRet != 0)
		 {
			 /**
			 自动重新连接
			 */
			 int iErrno = mysql_errno(_pstMql);
			 if (iErrno == 2013 || iErrno == 2006)
			 {
				 connect();
				 iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
			 }
		 }
		 
		 if (iRet != 0)
		 {
			 throw MysqlHelper_Exception("[MysqlHelper::execute]: mysql_query: [ " + sSql+" ] :" + string(mysql_error(_pstMql))); 
		 }
		 
		 MYSQL_RES *pstRes = mysql_store_result(_pstMql);
		 
		 if(pstRes == NULL)
		 {
		 	throw MysqlHelper_Exception("[MysqlHelper::queryRecord]: mysql_store_result: " + sSql + " : " + string(mysql_error(_pstMql)));
		 }
		 return pstRes;
	}

	bool MysqlHelper::queryRecordExist(const string& sSql)
	{
		 MYSQL_RES *pstRes = checkConnect(sSql);
		 
		 vector<string> vtFields;
		 MYSQL_FIELD *field;
		 while((field = mysql_fetch_field(pstRes)))
		 {
		 	vtFields.push_back(field->name);
		 }
		 
		 map<string, string> mpRow;
		 MYSQL_ROW stRow;
		 
		 while((stRow = mysql_fetch_row(pstRes)) != (MYSQL_ROW)NULL)
		 {
			 mysql_free_result(pstRes);
		 	 return true;
		 }
		 
		 mysql_free_result(pstRes);
		 
		 return false;
	}

	void MysqlHelper::createTable(const string &sTableName)
	{
        cout << sTableName << endl;
                string sSql;
        //string sSql = "drop table if exists " + sTableName + ";";
		//execute(sSql);
		if(sTableName.find("UserInfo") != string::npos)
		{
			sSql = "drop table if exists " + sTableName + ";";
		    execute(sSql);
			sSql = "create table " + sTableName + 
				" (id int(11) auto_increment not null, userId varchar(50) NOT NULL , telephone varchar(20),ipAddress varchar(20)," + 
				"channel varchar(50), mainChannel varchar(20), minorChannel varchar(20),minorType varchar(20), loginTime varchar(25), device int(11)," + 
				" isVip int(2), actions mediumtext CHARACTER SET utf8, PRIMARY KEY (id),INDEX index_tele(telephone))ENGINE=InnoDB DEFAULT CHARSET=utf8;";
            cout << "create : --- " << sSql << endl;
		}
		else if(sTableName.find("TotalAccess") != string::npos || sTableName.find("BlockAccess") != string::npos)
		{
			sSql = "create table IF NOT EXISTS " + sTableName + " (id int(11) auto_increment primary key not null, loginTime varchar(20), actionId int(11) NOT NULL , accessCount int(11));";
            //cout << "create : --- " << sSql << endl;
		}
		else if(sTableName.find("LineAccess") != string::npos)
		{
			sSql = "create table IF NOT EXISTS " + sTableName + " (id int(11) auto_increment primary key not null, loginTime varchar(20), actionId1 int(11) NOT NULL , actionId2 int(11) NOT NULL ,accessCount int(11));";
           // cout << "create : --- " << sSql << endl;
		}
		else if(sTableName.find("TotalStatistics") != string::npos)
		{
			sSql = "create table IF NOT EXISTS " + sTableName + " (id int(11) auto_increment primary key not null, date varchar(20), type smallint(6) NOT NULL COMMENT '0:APP  1:Web', " + 
				"allUser int(11) NOT NULL , allUserWoW float NOT NULL, allUserQoQ float NOT NULL, loginUser int(11) , loginUserWoW float NOT NULL, loginUserQoQ float NOT NULL, vipUser int(11), vipUserWoW float NOT NULL, vipUserQoQ float NOT NULL, " + 
				"payUser int(11) DEFAULT NULL COMMENT '实际支付人数', payUserWoW float NOT NULL, payUserQoQ float NOT NULL, regUser int(11) DEFAULT NULL COMMENT '注册人数', regUserWoW float NOT NULL, regUserQoQ float NOT NULL," +
				"todayToVip int(11) DEFAULT NULL COMMENT '今日注册并购买VIP人数',lastweekToVip int(11) DEFAULT NULL COMMENT '上一周内注册并购买VIP人数',lastmonthToVip int(11) DEFAULT NULL COMMENT '上一个月内注册并购买VIP人数',allToVip int(11) DEFAULT NULL COMMENT '今日总共购买VIP人数');";
           // cout << "create : --- " << sSql << endl;
		}
		execute(sSql);
	}
	
	bool MysqlHelper::comparison(Action& a,Action& b)
	{  
        return a.timestamp < b.timestamp;  
    }  

	size_t MysqlHelper::insertRecord(const string &sTableName, hash_map<string,UserOperate>  &mpColumns,int &loginNum,int &vipNum,int &regNum)
	{
	    string sSql;
        //遍历+插入记录
        for(hash_map<string,UserOperate>::iterator it = mpColumns.begin(); it != mpColumns.end(); it++)
        {
        	try
        	{
	        	//对用户的acitons，按时间戳进行排序
	        	sort(it->second.actions.begin(),it->second.actions.end()); 
	           	if(it->second.teleNumber != "") loginNum++;
	           	if(it->second.isVip) vipNum++;
	        	//将用户的actions组装成字符串，以“-”分割
	        	string userActions = "";
	        	string preStr = "";
	        	bool isReg = false;
	        	for(std::vector<Action>::iterator iter = it->second.actions.begin(); iter != it->second.actions.end(); iter++)
	        	{
	                stringstream stream;
				    stream<<(*iter).action;
				    string currentStr = stream.str();
				    if(!isReg && currentStr == "5301") {regNum++;isReg = true;}//统计当日注册用户
					if(currentStr == preStr) continue;
					//判断此action于前一个action是否为相同，若是，则略过
	        		userActions = userActions + stream.str() + "-";
	        		preStr = currentStr;
	        	}
	            /*if(userActions.size() <= 1) 
	            {
	            //    cout << "toooooshort " << userActions << endl;
	                continue;
	            }
	    		userActions.erase(userActions.size() - 1);
	    		//cout << userActions << endl;
	            if(userActions.size() >= 20000)
	            {
	           // cout << "toooooo longggggg " << userActions.size() << endl;
	           		continue;
	            }*/
	           	if(userActions.size() >= 1)
	           	{
	           		userActions.erase(userActions.size() - 1);
	           	}
	            stringstream stream;
			    stream<<it->second.isVip;

	        	sSql = "insert into " + sTableName + "(userId,telephone,ipAddress,channel,mainChannel,minorChannel,minorType,loginTime,device,isVip,actions) values( '";
	        	if(it->second.device == 1)
	        	{
	        		sSql = sSql + it->first + "','" + it->second.teleNumber + "','" + it->second.ipAddress + "','" + it->second.channel + "','" + it->second.mainCh + "','"+ it->second.minorCh + "','"+ it->second.minorType + "','"+ it->second.loginData + "',1," + stream.str() + ",'" + userActions + "')";
	        	}
	        	else
	        	{
	        		sSql = sSql + it->first + "','" + it->second.teleNumber + "','" + it->second.ipAddress + "','" + it->second.channel + "','"+ it->second.mainCh + "','"+ it->second.minorCh + "','"+ it->second.minorType + "','" + it->second.loginData + "',0," + stream.str() + ",'" + userActions+ "')";
	        	}
	        	//cout << sSql << endl;
				execute(sSql);
			}catch(exception &e)
            {
                cout << "insertRecord has error,continue" << endl;
                continue;
            } 
        }
	    return loginNum;
	}

	size_t MysqlHelper::insertRecordTotal(const string &sTableName, string loginDate, const bool isApp, vector<int> numVec)
	{
		int totalNum = numVec[0], loginNum = numVec[1], vipNum = numVec[2], payNum = numVec[3],regNum = numVec[4], todayToVip = numVec[5];
		int lastweekToVip = numVec[6],lastmonthToVip = numVec[7],allVip = numVec[8];
		int real_reg_app = numVec[9],total_user = numVec[10],vip_user = numVec[11],vip1_user = numVec[12],vip2_user = numVec[13],vip3_user = numVec[14];
		int actual_pay = numVec[15];
		//计算同比、环比
		string yeserday = getYesterday(loginDate);
		string lastweek = getLastWeek(loginDate);
		string type = "1";
		if(isApp) type = "0";

		string sSql = "select allUser,loginUser,vipUser,payUser,regUser,allToVip from " + sTableName + " where date = '" + yeserday + "' and type = " + type;
		cout << sSql << endl;
		hash_map<string,int> yeserdayMap = queryRecordQoQ(sSql);
		sSql = "select allUser,loginUser,vipUser,payUser,regUser,allToVip from " + sTableName + " where date = '" + lastweek + "' and type = " + type;
		hash_map<string,int> lastweekMap = queryRecordQoQ(sSql);

		

        //先判断此条数据是否已经存在于数据库中，若存在则更新，否则插入
		sSql = "select * from " + sTableName + " where date = '" + loginDate + "' and type = " + type;
		cout << sSql << endl;
		if(queryRecordExist(sSql))
		{
	    	sSql = "update " + sTableName + " set allUser = " + int_to_string(totalNum) + ",loginUser = " + int_to_string(loginNum) + ",vipUser = " + int_to_string(vipNum) + ",payUser = " + int_to_string(payNum) + ",regUser = " + int_to_string(regNum) + 
	    		",allUserWoW = " + float_to_string(calculateRate(lastweekMap,"allUser",totalNum)) + ",allUserQoQ = " + float_to_string(calculateRate(yeserdayMap,"allUser",totalNum)) + 
	    		",loginUserWoW = " + float_to_string(calculateRate(lastweekMap,"loginUser",loginNum)) + ",loginUserQoQ = " + float_to_string(calculateRate(yeserdayMap,"loginUser",loginNum)) +
	    		",vipUserWoW = " + float_to_string(calculateRate(lastweekMap,"vipUser",vipNum)) +",vipUserQoQ = " + float_to_string(calculateRate(yeserdayMap,"vipUser",vipNum)) +
	    		",payUserWoW = " + float_to_string(calculateRate(lastweekMap,"payUser",payNum)) +",payUserQoQ = " + float_to_string(calculateRate(yeserdayMap,"payUser",payNum)) +
	    		",regUserWoW = " + float_to_string(calculateRate(lastweekMap,"regUser",regNum)) +",regUserQoQ = " + float_to_string(calculateRate(yeserdayMap,"regUser",regNum)) +
	    		",todayToVip = " + int_to_string(todayToVip) + ",lastweekToVip = " + int_to_string(lastweekToVip) + ",lastmonthToVip = " + int_to_string(lastmonthToVip) + 
	    		",allToVip = " + int_to_string(allVip) + ",allToVipWoW = " + float_to_string(calculateRate(lastweekMap,"allToVip",allVip)) +",allToVipQoQ = " + float_to_string(calculateRate(yeserdayMap,"allToVip",allVip)) +
	    		",click_reg_user = " + int_to_string(real_reg_app) + ",total_user = " + int_to_string(total_user) + ",vip_user = " + int_to_string(vip_user) + ",vip1_user = " + int_to_string(vip1_user) +
	    		",vip2_user = " + int_to_string(vip2_user) + ",vip3_user = " + int_to_string(vip3_user) + ",actual_pay = " + int_to_string(actual_pay) +
	    		" where date = '" + loginDate + "' and type = " + type;
		}
		else
		{
    		sSql = "insert into " + sTableName + "(date,type, allUser,allUserWoW,allUserQoQ,loginUser,loginUserWoW,loginUserQoQ,vipUser,vipUserWoW,vipUserQoQ,payUser,payUserWoW,payUserQoQ," + 
    			"regUser,regUserWoW,regUserQoQ,todayToVip,lastweekToVip,lastmonthToVip,allToVip,allToVipWoW,allToVipQoQ,click_reg_user,total_user,vip_user,vip1_user,vip2_user,vip3_user,actual_pay) values( '";
    		sSql = sSql + loginDate + "'," + type + "," + int_to_string(totalNum) + "," + float_to_string(calculateRate(lastweekMap,"allUser",totalNum)) + ","  + float_to_string(calculateRate(yeserdayMap,"allUser",totalNum)) + "," +
    				int_to_string(loginNum) + "," + float_to_string(calculateRate(lastweekMap,"loginUser",loginNum)) + "," + float_to_string(calculateRate(yeserdayMap,"loginUser",loginNum)) + "," +
    				int_to_string(vipNum) + "," + float_to_string(calculateRate(lastweekMap,"vipUser",vipNum)) +"," + float_to_string(calculateRate(yeserdayMap,"vipUser",vipNum)) + "," +
    				int_to_string(payNum) + "," + float_to_string(calculateRate(lastweekMap,"payUser",payNum)) +"," + float_to_string(calculateRate(yeserdayMap,"payUser",payNum)) + "," +
    				int_to_string(regNum) + "," + float_to_string(calculateRate(lastweekMap,"regUser",regNum)) +"," + float_to_string(calculateRate(yeserdayMap,"regUser",regNum)) + "," +
    				int_to_string(todayToVip) + "," + int_to_string(lastweekToVip) + "," + int_to_string(lastmonthToVip) + "," + int_to_string(allVip) +  "," +
    				float_to_string(calculateRate(lastweekMap,"allToVip",allVip)) + "," + float_to_string(calculateRate(yeserdayMap,"allToVip",allVip)) + "," +
    				int_to_string(real_reg_app) + "," + int_to_string(total_user) + "," + int_to_string(vip_user) + "," + int_to_string(vip1_user) +  "," +
    				int_to_string(vip2_user) + "," + int_to_string(vip3_user) + "," + int_to_string(actual_pay) +
    				")";
		}
	    cout << sSql << endl;
		execute(sSql);
	 
	    return mysql_affected_rows(_pstMql);
	}

	size_t MysqlHelper::insertRecordTA(const string &sTableName, hash_map<int,int>  &mpColumns,string loginDate)
	{
	    string sSql;
            sSql = "delete from "+ sTableName + " where loginTime = " + loginDate;
            execute(sSql);

        //遍历+插入记录
        for(hash_map<int,int>::iterator it = mpColumns.begin(); it != mpColumns.end(); it++)
        {
                stringstream ss1,ss2;
                ss1<<it->first;
                ss2<<it->second;
        	sSql = "insert into " + sTableName + "(loginTime, actionId,accessCount) values( '";
        		sSql = sSql + loginDate + "'," + ss1.str() + "," + ss2.str() + ")";
			execute(sSql);
        }
	 
	    return mysql_affected_rows(_pstMql);
	}

	size_t MysqlHelper::insertRecordLine(const string &sTableName, hash_map<string,int>  &mpColumns,string loginDate)
	{
	    string sSql;
        sSql = "delete from "+ sTableName + " where loginTime = " + loginDate;
            execute(sSql);
        
        //遍历+插入记录
        for(hash_map<string,int>::iterator it = mpColumns.begin(); it != mpColumns.end(); it++)
        {
            string str1 = it->first.substr(0,4);
            string str2 = it->first.substr(5,4);
            stringstream ss2;
            ss2<<it->second;
        	sSql = "insert into " + sTableName + "(loginTime, actionId1,actionId2,accessCount) values( '";
        		sSql = sSql + loginDate + "'," + str1 + "," + str2 + "," + ss2.str() + ")";
			execute(sSql);
        }
	 
	    return mysql_affected_rows(_pstMql);
	}

    hash_map<string,TypeValue> MysqlHelper::queryRecord(const string& sSql)
    {
    	hash_map<string,TypeValue> urlInfo;
    	if(!_bConnected)
		{
		    connect();
		}
		_sLastSql = sSql;
 
		int iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		if(iRet != 0)
		{
		    /**
		     自动重新连接
		    */
		    int iErrno = mysql_errno(_pstMql);
		    if (iErrno == 2013 || iErrno == 2006)
		    {
		        connect();
		        iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		    }
		}
		 
		if (iRet != 0)
		{
		   throw MysqlHelper_Exception("[MysqlHelper::execute]: mysql_query: [ " + sSql+" ] :" + string(mysql_error(_pstMql))); 
		}

		MYSQL_RES *pstRes = mysql_store_result(_pstMql);
 
		if(pstRes == NULL)
		{
		    throw MysqlHelper_Exception("[MysqlHelper::queryRecord]: mysql_store_result: " + sSql + " : " + string(mysql_error(_pstMql)));
		}
		MYSQL_ROW stRow;
		while((stRow = mysql_fetch_row(pstRes)) != (MYSQL_ROW)NULL)
	    {
       		TypeValue typV;
       		typV.type = atoi(stRow[2]);
       		typV.urlID = atoi(stRow[1]);
	        urlInfo[stRow[0]] = typV;
	    }
	    mysql_free_result(pstRes);
	    return urlInfo;
    }

    vector<string> MysqlHelper::queryRecordChannel(const string& sSql)
    {
    	vector<string> urlInfo;
    	if(!_bConnected)
		{
		    connect();
		}
		_sLastSql = sSql;
 
		int iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		if(iRet != 0)
		{
		    /**
		     自动重新连接
		    */
		    int iErrno = mysql_errno(_pstMql);
		    if (iErrno == 2013 || iErrno == 2006)
		    {
		        connect();
		        iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		    }
		}
		 
		if (iRet != 0)
		{
		   throw MysqlHelper_Exception("[MysqlHelper::execute]: mysql_query: [ " + sSql+" ] :" + string(mysql_error(_pstMql))); 
		}

		MYSQL_RES *pstRes = mysql_store_result(_pstMql);
 
		if(pstRes == NULL)
		{
		    throw MysqlHelper_Exception("[MysqlHelper::queryRecord]: mysql_store_result: " + sSql + " : " + string(mysql_error(_pstMql)));
		}
		MYSQL_ROW stRow;
		while((stRow = mysql_fetch_row(pstRes)) != (MYSQL_ROW)NULL)
	    {
	            urlInfo.push_back(stRow[0]);
	    }
	    mysql_free_result(pstRes);
	    return urlInfo;
    }

    hash_map<string,int> MysqlHelper::queryRecordVIP(const string& sSql)
    {
    	hash_map<string,int> userInfo;
    	if(!_bConnected)
		{
		    connect();
		}
		_sLastSql = sSql;
 
		int iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		if(iRet != 0)
		{

		    int iErrno = mysql_errno(_pstMql);
		    if (iErrno == 2013 || iErrno == 2006)
		    {
		        connect();
		        iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		    }
		}
		 
		if (iRet != 0)
		{
		   throw MysqlHelper_Exception("[MysqlHelper::execute]: mysql_query: [ " + sSql+" ] :" + string(mysql_error(_pstMql))); 
		}

		MYSQL_RES *pstRes = mysql_store_result(_pstMql);
 
		if(pstRes == NULL)
		{
		    throw MysqlHelper_Exception("[MysqlHelper::queryRecord]: mysql_store_result: " + sSql + " : " + string(mysql_error(_pstMql)));
		}
		MYSQL_ROW stRow;
		while((stRow = mysql_fetch_row(pstRes)) != (MYSQL_ROW)NULL)
	    {
	            userInfo[stRow[0]] = 1;
	    }
	    mysql_free_result(pstRes);
	    return userInfo;
    }

    int MysqlHelper::queryRecordPayNum(const string& sSql)
    {
    	if(!_bConnected)
		{
		    connect();
		}
		_sLastSql = sSql;
 
		int iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		if(iRet != 0)
		{

		    int iErrno = mysql_errno(_pstMql);
		    if (iErrno == 2013 || iErrno == 2006)
		    {
		        connect();
		        iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		    }
		}
		 
		if (iRet != 0)
		{
		   throw MysqlHelper_Exception("[MysqlHelper::execute]: mysql_query: [ " + sSql+" ] :" + string(mysql_error(_pstMql))); 
		}

		MYSQL_RES *pstRes = mysql_store_result(_pstMql);
 
		if(pstRes == NULL)
		{
		    throw MysqlHelper_Exception("[MysqlHelper::queryRecord]: mysql_store_result: " + sSql + " : " + string(mysql_error(_pstMql)));
		}
		MYSQL_ROW stRow;
		while((stRow = mysql_fetch_row(pstRes)) != (MYSQL_ROW)NULL)
	    {
	         mysql_free_result(pstRes);
	         if(stRow[0] == NULL) return 0;
	         return atoi(stRow[0]);
	    }
	    mysql_free_result(pstRes);
	    return 0;
    }

    hash_map<string,int> MysqlHelper::queryRecordQoQ(const string& sSql)
    {
    	hash_map<string,int> userInfo;
    	/*userInfo["allUser"] = 0;
        userInfo["loginUser"] = 0;
        userInfo["vipUser"] = 0;
        userInfo["payUser"] = 0;
        userInfo["regUser"] = 0;*/
    	if(!_bConnected)
		{
		    connect();
		}
		_sLastSql = sSql;
 
		int iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		if(iRet != 0)
		{

		    int iErrno = mysql_errno(_pstMql);
		    if (iErrno == 2013 || iErrno == 2006)
		    {
		        connect();
		        iRet = mysql_real_query(_pstMql, sSql.c_str(), sSql.length());
		    }
		}
		 
		if (iRet != 0)
		{
		   throw MysqlHelper_Exception("[MysqlHelper::execute]: mysql_query: [ " + sSql+" ] :" + string(mysql_error(_pstMql))); 
		}

		MYSQL_RES *pstRes = mysql_store_result(_pstMql);
 
		if(pstRes == NULL)
		{
		    throw MysqlHelper_Exception("[MysqlHelper::queryRecord]: mysql_store_result: " + sSql + " : " + string(mysql_error(_pstMql)));
		}
		MYSQL_ROW stRow;
		while((stRow = mysql_fetch_row(pstRes)) != (MYSQL_ROW)NULL)
	    {
	        userInfo["allUser"] = atoi(stRow[0]);
	        userInfo["loginUser"] = atoi(stRow[1]);
	        userInfo["vipUser"] = atoi(stRow[2]);
	        userInfo["payUser"] = atoi(stRow[3]);
	        userInfo["regUser"] = atoi(stRow[4]);
	        userInfo["allToVip"] = atoi(stRow[5]);
	    }
	    mysql_free_result(pstRes);
	    return userInfo;
    }

    string MysqlHelper::getYesterday(string input)
	{
	    tm tm_;  
	    time_t t_;  
	    char buf[128]= {0};  
	    input = input.substr(0,4) + "-" + input.substr(4,2) + "-" + input.substr(6,2);
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

	string MysqlHelper::getLastWeek(string input)
	{
	    tm tm_;  
	    time_t t_;  
	    char buf[128]= {0};  
	    input = input.substr(0,4) + "-" + input.substr(4,2) + "-" + input.substr(6,2);
	    string tmpStr = input + " 14:00:00";
	    strcpy(buf, tmpStr.c_str());  
	    strptime(buf, "%Y-%m-%d %H:%M:%S", &tm_); //将字符串转换为tm时间  
	    tm_.tm_isdst = -1;  
	    tm_.tm_mday = tm_.tm_mday - 7;
	    t_  = mktime(&tm_); //将tm时间转换为秒时间   
	      
	    tm_ = *localtime(&t_);//输出时间  
	    strftime(buf, 64, "%Y%m%d %H:%M:%S", &tm_);  
	    string nextDay = buf;
	    nextDay = nextDay.substr(0,8);
	    std::cout << nextDay << std::endl;
	    return nextDay;
	}

	string MysqlHelper::int_to_string(int iValue)
	{
	    stringstream stream;  
	    stream << iValue;
	    return stream.str();
	}
	string MysqlHelper::float_to_string(float iValue)
	{
	    stringstream stream;  
	    stream << iValue;
	    return stream.str();
	}

	double MysqlHelper::calculateRate(hash_map<string,int> map,string key,int value)
	{
	    //计算变化率
	    hash_map<string,int>::iterator iter;
	    iter = map.find(key);
	    if(iter != map.end())
	    {
	        //计算变化率：rate = (X - A)/A
	        if(map[key] != 0)
	        {
	            double tmp = (double)((value - map[key]) * 1.0/map[key]) * 100;
	            char buf[10];
	            sprintf(buf, "%.2f", tmp);
	            return atof(buf);
	        }
	        else
	        {
	            return INT_MAX;
	        }
	    }
	    else
	    {
	        //没有昨日数据的话变化率为100%
	        //cout << "yesterday UserInfo do not contain : " << key << endl;
	        return INT_MAX;
	    }
	}
}
