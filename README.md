# 基于用户nginx日志，解析用户行为，将用户各行为解析为对应编号，最终以编号组合的形式存储到MYSQL数据库中。

LogAnalyze.cpp
程序主入口，包含从读取用户日志、用户日志解析到入库的全部过程。

MysqlHelper.cpp & MysqlHelper.h
数据库管理类，包含数据库相关所有操作。

UserOperate.cpp & UserOperate.h
用户信息类，定义用户信息及用户操作相关方法。

base64.cpp & base64.h
含base64转中文、中文转base64方法。

conf/
包含所有配置信息。