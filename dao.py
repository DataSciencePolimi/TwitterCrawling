import sqlite3
import logging as logger
logger.basicConfig(format="%(asctime)s %(message)s", level=logger.DEBUG,filename="/Users/emrecalisir/git/out.log")


def create_db_connection(db_file):
   conn = None
   try:
      conn = sqlite3.connect(db_file)
      cursor = conn.cursor()
      cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
      logger.info(cursor.fetchall())
   except Exception as e:
      logger.error(e)

   return conn


def get_running_crawlers(conn):
   rows = []
   try:
      cur = conn.cursor()
      cur.execute("SELECT * FROM running_crawlers")

      rows = cur.fetchall()
      if (len(rows)==0):
         logger.info("No record found for any spider")

   except Exception as ex:
      logger.error("exception" + ex)

   return rows


def is_any_crawler_running_for_date(conn, target_date):
   res = False
   try:
      cur = conn.cursor()
      cur.execute("SELECT * FROM running_crawlers where target_date=?",(target_date,))

      rows = cur.fetchall()
      if (len(rows)==0):
         logger.info("No record found for given target_date: " + str(target_date))
         return False
      if (len(rows)>1):
         logger.info("Inconsistency, place Id field should be UNIQUE but found more than 1 records for target_date id: " + str(target_date) )
         return False

      row = rows[0]
      logger.info("Record found: " + str(row))
      res = True
   except Exception as ex:
      logger.error("exception" + str(ex))

   return res


def check_running_spider_for_date(conn, target_date):
   res = False
   try:
      cur = conn.cursor()
      cur.execute("SELECT * FROM running_crawlers where target_date=?",(str(target_date),))

      rows = cur.fetchall()
      if (len(rows)==0):
         logger.info("No record found for given target_date: " + str(target_date))
         return False

      row = rows[0]
      logger.info("Record found: " + str(row))
      res = True
   except Exception as ex:
      logger.error("exception" + ex)

   return res


def insert_to_db_new_crawler_process(conn, os_process_id, target_date):
   id = None
   try:
      cur = conn.cursor()

      cur.execute("INSERT INTO running_crawlers(os_process_id, target_date) VALUES(?,?)",(str(os_process_id), str(target_date)) )
      id = cur.lastrowid
      conn.commit()
      logger.info("insert to db success for os process id: " + str(os_process_id) + " and the end date: " + str(target_date))
   except Exception as ex:
      logger.error("exception" + str(ex))
   return id