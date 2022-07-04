#! /usr/bin/env python
# coding=utf-8

import sys
import datetime
import json
from aliyunsdkcore import client
import pymysql
import os
import binascii
from aliyunsdkrds.request.v20140815 import DescribeSlowLogRecordsRequest
import ConfigParser
import threading
import time
import re
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import html_slowlog
import logging
from warnings import filterwarnings


reload(sys)
sys.setdefaultencoding("utf-8")

filterwarnings('error', category=pymysql.Warning)

config = ConfigParser.ConfigParser()
config.read("/opt/mysql-tool/hostid.cfg")

logger = logging.getLogger()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s: %(message)s',
    filename='/tmp/slowlog_error.log',
    filemode='w')

AccessKeyId = dict(config.items('alikey')).get('accesskeyid')
AccessKeySecret = dict(config.items('alikey')).get('accesskeysecret')

clt = client.AcsClient(AccessKeyId, AccessKeySecret, 'cn-hangzhou')

now = datetime.datetime.now()
# utc_date=now+datetime.timedelta(hours=-32)
# time_rang=now+datetime.timedelta(hours=-8)
# starttime=utc_date.strftime('%Y-%m-%dT%H:%MZ')
# endtime=time_rang.strftime('%Y-%m-%dT%H:%MZ')
e = now + datetime.timedelta(days=-1)
s = now + datetime.timedelta(days=-2)
starttime = s.strftime('%Y-%m-%dT16:00Z')
endtime = e.strftime('%Y-%m-%dT16:00Z')




def mysql_conn(USER, PASSWORD, IP, PORT, dbname='mysql'):
    db = pymysql.connect(user=USER, passwd=PASSWORD, host=IP, port=int(PORT), charset="UTF8", db=dbname,
                         client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
                         autocommit=True)
    con = db.cursor()
    return con


def get_slow_log_info(starttime, endtime, hostid):
    request = DescribeSlowLogRecordsRequest.DescribeSlowLogRecordsRequest()
    request.set_DBInstanceId(hostid)
    request.set_StartTime(starttime)
    request.set_EndTime(endtime)
    # request.set_PageNumber(2)
    request.set_PageSize(100)
    request.set_accept_format('json')
    result = clt.do_action(request)
    result_dict = json.loads(result, encoding="UTF-8")

    page_number = (result_dict[u'TotalRecordCount'] + 100 - 1) / 100
    TotalRecordCount = result_dict[u'TotalRecordCount']
    return page_number, TotalRecordCount


def get_slow_log(starttime, endtime, hostid, PageNumber):
    request = DescribeSlowLogRecordsRequest.DescribeSlowLogRecordsRequest()
    request.set_DBInstanceId(hostid)
    request.set_StartTime(starttime)

    request.set_EndTime(endtime)
    request.set_PageNumber(PageNumber)
    request.set_PageSize(100)
    request.set_accept_format('json')
    result = clt.do_action(request)
    result_dict = json.loads(result, encoding="UTF-8")
    return result_dict[u'Items'][u'SQLSlowRecord']


def crc32(str):

    return binascii.crc32(str) & 0xffffffff


def sql_genre(sqltext):
    com = """/usr/bin/pt-fingerprint --match-embedded-numbers --match-md5-checksums --query "%s" """ % (sqltext.replace(r'`', '').replace(r'"', "'"))
    # print com
    return os.popen(com).read()


def cst_date(utc_time):
    try:
        cst_time = datetime.datetime.strptime(str(utc_time), '%Y-%m-%dt%H:%M:%Sz')
        cst_time = cst_time + datetime.timedelta(hours=8)
        return str(cst_time)
    except BaseException as e:
        loginfo = "trans cst time error %s" % (e)
        print(loginfo)
        print(utc_time)


def sql_explain(sql, USER, PASSWORD, IP, PORT, dbname, sql_crc32):
    db_connect = mysql_conn(USER, PASSWORD, IP, PORT, dbname)
    explain_sql = """explain %s""" % (sql)
    # print (explain_sql)
    try:
        db_connect.execute(explain_sql)
        explain_crc32 = 0
        sql_cmd = []
        for i in db_connect.fetchall():
            row_dict = {}

            row_dict['id'] = i[0]
            row_dict['select_type'] = i[1]
            row_dict['table'] = i[2]
            row_dict['type'] = i[3]
            row_dict['possible_keys'] = i[4]
            row_dict['key'] = i[5]
            row_dict['key_len'] = i[6]
            row_dict['ref'] = i[7]
            row_dict['rows'] = i[8]
            row_dict['extra'] = i[9]
            sql = """replace into slowlog.explain_sql(`crc32`,`sql_explain_crc32`,`sql_explain_crc32_sum`,`id`,`select_type`,`table`,`type`,`possible_keys`,
                    `key`,`key_len`,`ref`,
                             `rows`,`extra`) values ("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s");""" % (
            sql_crc32, crc32(str(i)),
            'sql_explain_crc32_sum_replace',
            i[0], i[1], i[2], i[3],
            i[4], i[5], i[6], i[7], i[8], i[9])
            explain_crc32 += crc32(str(i))
            sql_cmd.append(sql)

        sql_list = [i.replace('sql_explain_crc32_sum_replace', str(explain_crc32)) for i in sql_cmd]
        return sql_list
        db_connect.close()
    except db_connect.Error as e:
        logger.info (explain_sql)
        logger.info (e)
        db_connect.close()

    except db_connect.Warning as e:
        logger.info (explain_sql)
        logger.info (e)
        db_connect.close()


def in_db(sqlinfo, instan):
    db = pymysql.connect(user='xxx', passwd='xxx', host='xxx', port=3306, db='slowlog',
                         charset="UTF8", autocommit=True)
    for row in sqlinfo:
        searchObj = re.search(r'(.*)information_schema(.*)|commit|prepare|.*DMS-E.*|show .*|call .*|Binlog Dump .*|alter .*', row[u'SQLText'].strip(),
                              re.M | re.I)

        if searchObj == None:
            sql_crc32 = crc32(sql_genre(row[u'SQLText']))
            sql = """insert into slowlog_info(HostAddress,DBName,Instan_Name,SQLText,Or_sqltext,QueryTimes,LockTimes,ParseRowCounts,ReturnRowCounts,
            ExecutionStartTime,
            CST_ExecutionStartTime,crc32)
                values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")
                """ % (
            row[u'HostAddress'], row[u'DBName'], instan, sql_genre(row[u'SQLText']), row[u'SQLText'].replace(r'"', "'"), row[u'QueryTimes'],
            row[u'LockTimes'], row['ParseRowCounts'], row[u'ReturnRowCounts'], row[u'ExecutionStartTime'], cst_date(row[u'ExecutionStartTime']),
            sql_crc32)
            # print sql
            # insert into slowlog to db
            try:
                con = db.cursor()
                con.execute(sql)

            except con.Error as e:
                loginfo = "database error:%s" % e
                print(loginfo)
                print(sql)
                con.close()


            IP = config.get('dbinstances', instan).split(':')[1]
            PORT = config.get('dbinstances', instan).split(':')[2]
            USER = config.get('dbinstances', instan).split(':')[3]
            PASSWORD = config.get('dbinstances', instan).split(':')[4]
            # print (USER)
            # print (PASSWORD)
            dbname = row[u'DBName']
            sql = row[u'SQLText']
            # print sql
            list_sql = sql_explain(sql, USER, PASSWORD, IP, PORT, dbname, sql_crc32)
            if list_sql:
                for i in list_sql:
                    try:
                        # print i
                        con.execute(i)
                    except con.Error as e:
                        print (e)
                        print (i)
                        con.close()
                    except con.Warning as (w):
                        print (w)
                        print (i)
            con.close()


class mythread(threading.Thread):
    def __init__(self, threadID, instan, slowlog_info, hostid):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.instan = instan
        self.slowlog_info = slowlog_info
        self.hostid = hostid


    def run(self):
        loginfo = "starting " + self.instan + " thread " + time.ctime()

        print(loginfo)
        if self.slowlog_info[1] > 0:
            loginfo = "starting analyze %s slowlog! total_number:%d" % (self.instan, self.slowlog_info[1])
            print (loginfo)
            logging.info(loginfo)
            for number in range(1, int(self.slowlog_info[0]) + 1):
                slow_log = get_slow_log(starttime, endtime, self.hostid, number)

                loginfo = self.instan + ' finish percent:' + str(int(float(number) / self.slowlog_info[0] * 100)) + '%'
                print (loginfo)
                in_db(slow_log, self.instan)
            loginfo = "analyze %s slowlog succeed" % (self.instan)

            print(loginfo)
        else:
            loginfo = "%s this is no slowlog" % (self.instan)
            print(loginfo)


def send_emaile():
    now = datetime.datetime.now()
    yesterday = now + datetime.timedelta(days=-1)
    day_8ago = now + datetime.timedelta(days=-8)
    now_f = now.strftime("%Y-%m-%d")
    yesterday_f = yesterday.strftime("%Y-%m-%d")
    day_8ago = day_8ago.strftime("%Y-%m-%d")
    db = pymysql.connect(user='root', passwd='admin', host='10.16.18.3', port=3306, db='slowlog',
                         charset="UTF8")
    con = db.cursor()
    sql_slow = """ 
        SELECT 
                COUNT(*) AS count,
                CST_ExecutionStartTime,
                AVG(querytimes) AS avg_querytimes,
                MAX(querytimes) AS max_querytimes,
                crc32 AS fingerprint,
                HostAddress,
                Instan_Name,
                dbname,
                ParseRowCounts,
                ReturnRowCounts,
                or_sqltext
                FROM
                slowlog_info
                WHERE
                CST_ExecutionStartTime >= '%s 00:00:00'
                AND CST_ExecutionStartTime < '%s'
                
                and substring_index(HostAddress, '[', 1) not in (
                                                   
                                                   'bling',
                                                   'zhoujin'
                                                            )
                
                GROUP BY crc32
                HAVING avg_querytimes > 2
                ORDER BY max_querytimes DESC;
     """ % (yesterday_f, now_f)

    # print sql_slow
    sql_newslowlog = """
        SELECT 
            COUNT(*) AS count,
            cst_ExecutionStartTime,
            avg(querytimes) AS avg_querytimes,
            crc32 AS fingerprint,
            HostAddress,
            Instan_Name,
            or_sqltext
        FROM
            slowlog_info b
        WHERE
            b.CST_ExecutionStartTime >= '%s 07:00:00'
                AND b.CST_ExecutionStartTime < '%s'
                AND b.crc32 NOT IN (SELECT DISTINCT
                    (a.crc32)
                FROM
                    slowlog_info a
                WHERE
                    a.CST_ExecutionStartTime >= '%s'
                    AND a.CST_ExecutionStartTime < '%s')
		    and (HostAddress not like 'ec_aliyun%%' and HostAddress not like 'ecdba%%')
        GROUP BY crc32 
	HAVING avg_querytimes > 2 and count > 10
        ORDER BY 1 DESC
    """ % (yesterday_f, now_f, day_8ago, yesterday_f)

    sql_full_scan = """select COUNT(*) AS count,
       		a.CST_ExecutionStartTime,
       		avg(a.querytimes) AS avg_querytimes,
            max(a.querytimes) AS max_querytimes,
       		a.crc32 AS fingerprint,
       		a.HostAddress,
       		a.Instan_Name,
            a.DBName,
            a.ParseRowCounts,
            a.ReturnRowCounts,
       		a.or_sqltext
  		from slowlog_info a
 		where 
		a.CST_ExecutionStartTime >= '%s'
                AND a.CST_ExecutionStartTime < '%s'
                and (a.HostAddress not like 'ec_aliyun%%' and HostAddress not like 'ecdba%%')
		and a.`crc32`
		in(
		select DISTINCT(`crc32`)
  		from explain_sql
 		where type= 'ALL'
   		and `select_type`!= 'UNION RESULT')
 		GROUP BY a.`crc32`
 		having avg_querytimes >3 
 		ORDER BY count desc;
		""" % (yesterday_f, now_f)

    # print (sql_full_scan)

    con.execute(sql_slow)
    db_result = con.fetchall()
    slowlog_data = []
    for row in db_result:
        w = ['<td>' + str(rows) + '</td>' for rows in row]
        w = " ".join(map(str, w))
        slowlog_data.append('<tr>' + str(w[::]) + '</tr>')

    # new_slowlog_data = []
    # con.execute(sql_newslowlog)
    # db_result2 = con.fetchall()
    # for row in db_result2:
    #     w = ['<td>' + str(rows) + '</td>' for rows in row]
    #     w = " ".join(map(str, w))
    #     new_slowlog_data.append('<tr>' + str(w[::]) + '</tr>')
    #
    full_scan_data = []
    con.execute(sql_full_scan)
    for row in con.fetchall():
        w = ['<td>' + str(rows) + '</td>' for rows in row]
        w = " ".join(map(str, w))
        full_scan_data.append('<tr>' + str(w[::]) + '</tr>')

    html_body = html_slowlog.html_fomat(slowlog_data, full_scan_data)
    # html_body = html_slowlog.html_fomat(slowlog_data)

    con.close()
    # print html_body
    mail_dict = dict(config.items('mail_config'))
    passwrod = mail_dict['mail_pass']
    sener = mail_dict['mail_user']
    receivers = mail_dict['mail_receiver'].split(";")
    message = MIMEText(html_body, 'html', 'utf-8')
    message['From'] = Header("DBA", 'utf-8')
    # message['To'] = Header("OPS", 'utf-8')
    subject = '(%s) SLOWLOG INFO(生产环境慢查询)' % (yesterday_f)
    message['Subject'] = Header(subject, 'utf-8')

    try:
        smtpObj = smtplib.SMTP_SSL('smtp.mxhichina.com', 465)
        # smtpObj.starttls()
        smtpObj.login(sener, passwrod)
        smtpObj.sendmail(sener, receivers, message.as_string())
        print ("email is send sucess")
    except smtplib.SMTPException as err:
        print (err)


if __name__ == '__main__':

    config_instances = dict(config.items('dbinstances'))
    print ("""##########################################""")
    loginfo = "starttime:%s " % (now)
    print (loginfo)
    for instan, hostid in config_instances.items():

        hostid = hostid.split(':')[0]
        slowlog_info = get_slow_log_info(starttime, endtime, hostid)
        thread = mythread("thread" + str(instan), str(instan), slowlog_info, hostid)
        thread.start()
        thread.join()
    send_emaile()
