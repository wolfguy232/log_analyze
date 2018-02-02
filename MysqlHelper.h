//mysqlhelper.h
#include <stdlib.h>
using namespace std;
#include <mysql/mysql.h>
#include "UserOperate.h"
#include <ext/hash_map>
#include <map>
using namespace __gnu_cxx;

namespace mysqlhelper
{
	/*********************
	*@brief 数据库异常类
	**********************/
	struct MysqlHelper_Exception //: public TC_Exception
	{
	 MysqlHelper_Exception(const string &sBuffer):errorInfo(sBuffer){}; //: TC_Exception(sBuffer){};
	 ~MysqlHelper_Exception() throw(){}; 
	 
	 string errorInfo;
	};

	/*****************
	* @brief 数据库配置接口
	******************/
	struct DBConf
	{
		string _host;//主机地址
		string _user;//用户名
		string _password;//密码
		string _database;//DB
		string _charset;//字符集
		int _port;//端口
		int _flag;//客户端标识

		//构造函数
		DBConf():_port(0),_flag(0){}
	};

	/*****************
	* @brief MySQL数据库操作类
	******************/
	class MysqlHelper
	{
	public:
		MysqlHelper();
		~MysqlHelper();
		MysqlHelper(const string& sHost,const string& sUser = "",const string& sPasswd = "",const string&sDatabase = "", 
			const string& sCharSet = "", int port = 0, int iFlag = 0);

		MysqlHelper(const DBConf& tcDBConf);

		//初始化
		void init(const string& sHost, const string& sUser = "", const string& sPasswd = "", const string& sDatabase = "", 
			const string &sCharSet = "", int port = 0, int iFlag = 0);

		void init(const DBConf& tcDBConf);

		//连接数据库
		void connect();

		//断开数据库连接
		void disconnect();

		MYSQL_RES * checkConnect(const string& sSql);

		//获取数据库指针
		MYSQL *getMysql();

	    //更新或者插入数据
		void execute(const string& sSql);
                
        //创建表
        void createTable(const string& sTableName);

        //比较函数--用于排序
        bool comparison(Action& a,Action& b);
        bool queryRecordExist(const string& sSql);
                
		//插入数据
		size_t insertRecord(const string &sTableName, hash_map<string,UserOperate> &mpColumns,int &loginNum,int &vipNum,int &regNum);
		size_t insertRecordTA(const string &sTableName, hash_map<int,int> &mpColumns,string loginDate);
		size_t insertRecordLine(const string &sTableName, hash_map<string,int> &mpColumns,string loginDate);
		size_t insertRecordTotal(const string &sTableName, string loginDate, const bool isApp, vector<int> numVec);

		//更新数据
		//size_t updateRecord();

		//删除数据
		size_t deleteRecord(const string &sTableName, const string &sCondition = "");

        //获取dict表信息
        hash_map<string,TypeValue> queryRecord(const string& sSql);
        //vector<int> queryRecordBlock(const string& sSql);
        vector<string> queryRecordChannel(const string& sSql);
        hash_map<string,int> queryRecordVIP(const string& sSql);
        hash_map<string,int> queryRecordQoQ(const string& sSql);
        int queryRecordPayNum(const string& sSql);

        string getYesterday(string input);
        string getLastWeek(string input);

        string int_to_string(int iValue);
		string float_to_string(float iValue);
		double calculateRate(hash_map<string,int> map,string key,int value);

	private:
	    //数据库指针
	    MYSQL *_pstMql;

		//数据库配置
		DBConf _dbConf;

		//是否已连接
		bool _bConnected;

		//最后执行的sql
		string _sLastSql;
	};
}
