import MySQLdb   #yum install MySQL-python
import datetime
import time
import sys
import os
import fcntl

mysql_ip_src = sys.argv[1]
mysql_username_src = sys.argv[2]
mysql_pass_src = sys.argv[3]
mysql_port_src = int(sys.argv[4])
mysql_dbname_src = sys.argv[5]

mysql_ip_des = sys.argv[6]
mysql_username_des = sys.argv[7]
mysql_pass_des = sys.argv[8]
mysql_port_des = int(sys.argv[9])
mysql_dbname_des = sys.argv[10]

days_before_to_backup = int(sys.argv[11])

print mysql_ip_src,mysql_username_src,mysql_pass_src,mysql_port_src,mysql_dbname_src, mysql_ip_des,mysql_username_des,mysql_pass_des,mysql_port_des,mysql_dbname_des

def get_days_ago(d):
    return (int)(time.mktime((datetime.datetime.now() - datetime.timedelta(days=d)).timetuple()))

def get_min_click(srccur, srcconn, tablename):
    sql = 'select min(clock) from '+ tablename
    print sql

    count = srccur.execute(sql)
    #print '['+tablename+']', 'row count =', count
    result = srccur.fetchone()
    print "min click:",result

    if result is None:
       result = 0
    else:
       result = result[0]

    return (int)(result);

def backuptable(srccur, srcconn, descur, desconn, tablename, days):
    total_count = 0;
    first = get_days_ago(days);
    print days," ago:",first
    min_clock = get_min_click(srccur, srcconn, tablename);
    print "min clock:",min_clock
    if min_clock is 0:
       return 0;
    
    second = first-3600;
    while True:
        if min_clock > first:
            break;

        total_count = total_count + backuptable_range(srccur, srcconn, descur, desconn, tablename, first, second)
        first = second
        second = second-3600
    print tablename," total backup count:",total_count
    return total_count

def backuptable_range(srccur, srcconn, descur, desconn, tablename, start_timestamp, end_timestamp):
   sql = 'select * from '+ tablename +' where clock < %s and clock > %s'
   print sql

   count = srccur.execute(sql, (start_timestamp, end_timestamp))
   print '['+tablename+']',  start_timestamp, end_timestamp, 'row count =', count

   if count<=0:
     return 0
   
   backed_count = 0;

#   try:
#       rows = srccur.fetchall()   
#       sql = 'insert into '+ tablename +' values(%s, %s, %s, %s) '
#       backed_count = backed_count + descur.executemany(sql, rows)
#       desconn.commit()

#   except MySQLdb.Error,e:
#       desconn.rollback()
#       print sql
#       print "Mysql Error %d: %s" % (e.args[0], e.args[1])
#       return backed_count;

   while True:
     rows = srccur.fetchmany(100)
     if not rows:
        break

     try:
         sql = 'insert into '+ tablename +' values(%s, %s, %s, %s) '
         backed_count = backed_count + descur.executemany(sql, rows)
         desconn.commit()

     except MySQLdb.Error,e:
       desconn.rollback()
       print sql
       print "Mysql Error %d: %s" % (e.args[0], e.args[1])
       return backed_count;
   
   #delete from src tables
   sql = 'delete from '+ tablename +' where clock < %s and clock > %s'
   srccur.execute(sql, (start_timestamp, end_timestamp))
   srcconn.commit()
   
   #print "count:",  backed_count
   return backed_count


def backup(timestamp): 
  try:
    print "start backup..."
    desconn=MySQLdb.connect(mysql_ip_des,mysql_username_des,mysql_pass_des,mysql_dbname_des,mysql_port_des)    
    srcconn=MySQLdb.connect(mysql_ip_src,mysql_username_src,mysql_pass_src,mysql_dbname_src,mysql_port_src)
    srccur=srcconn.cursor()
    descur=desconn.cursor()
    #backuptable(srccur, srcconn, descur, desconn, 'history_str', timestamp)
    backuptable(srccur, srcconn, descur, desconn, 'history', timestamp)
    #backuptable(srccur, srcconn, descur, desconn, 'history_uint', timestamp)
    #backuptable(srccur, srcconn, descur, desconn, 'history_text', timestamp)
    #backuptable(srccur, srcconn, descur, desconn, 'history_log', timestamp)
    srccur.close()
    srcconn.close()
    descur.close()
    desconn.close()

    print "end backup..."
    return 0

  except MySQLdb.Error,e:
     print "Mysql Error %d: %s" % (e.args[0], e.args[1])

  return -1
     
fh=0

def run_once():
    global fh
    fh=open(os.path.realpath(__file__),'r')
    try:
        fcntl.flock(fh,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except:
        print "try to exit..."
        os._exit(0)


if __name__ == '__main__':
    run_once()
    print "-----------start at",time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),"----------------"
    backup(days_before_to_backup)
    print "-----------end at",time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),"----------------"
