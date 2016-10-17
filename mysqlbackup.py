import MySQLdb
import datetime
import time

def backuptable(srccur, srcconn, descur, desconn, tablename):
   currentTime = int(time.mktime(datetime.datetime.now().timetuple()))
   print currentTime

   sql = 'select * from '+ tablename +' where clock < %s'
   srccur.execute(sql, currentTime)
   rows = srccur.fetchall()
   print rows

   sql = 'insert into '+ tablename +' values(%s, %s, %s, %s) '
   descur.executemany(sql, rows)
   desconn.commit()
   
   sql = 'delete from '+ tablename +' where clock < %s'
   srccur.execute(sql, currentTime)
   srcconn.commit()
   return 0;


def backup(): 
  try:
    print "start backup..."
    desconn=MySQLdb.connect(host='192.168.216.138',user='zabbix',passwd='zabbix',db='zabbix',port=3306)    
    srcconn=MySQLdb.connect(host='192.168.216.137',user='zabbix',passwd='zabbix',db='zabbix',port=3306)
    srccur=srcconn.cursor()
    descur=desconn.cursor()
    backuptable(srccur, srcconn, descur, desconn, 'history_str')
    backuptable(srccur, srcconn, descur, desconn, 'history')
    backuptable(srccur, srcconn, descur, desconn, 'history_uint')
    backuptable(srccur, srcconn, descur, desconn, 'history_text')
    backuptable(srccur, srcconn, descur, desconn, 'history_log')
    srccur.close()
    srcconn.close()
    descur.close()
    desconn.close()

    print "end backup..."
    return 0

  except MySQLdb.Error,e:
     print "Mysql Error %d: %s" % (e.args[0], e.args[1])

  return -1

if __name__ == '__main__':
   while(1):
     backup()
     time.sleep(10)
     #time.sleep(60*60*24)
