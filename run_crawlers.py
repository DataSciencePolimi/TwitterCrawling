from pymongo import MongoClient
import datetime
import time
import logging as logger
logger.basicConfig(format="%(asctime)s %(message)s", level=logger.DEBUG,filename="/Users/emrecalisir/git/out.log")
import subprocess
import dao
from datetime import datetime, timedelta


def format_date(year_start, year_end, month_start, month_end, day_start, day_end):
    day_start = str(day_start).rjust(2, '0')
    day_end = str(day_end).rjust(2, '0')

    month_start = str(month_start).rjust(2, '0')
    month_end = str(month_end).rjust(2, '0')

    target_date = str(year_start) + "_" + str(year_end) + "_" + str(month_start) + "_" + str(month_end) + "_" + str(day_start) + "_" + str(day_end)
    return target_date


#def get_available_spiders(running_spiders, all_spiders):
#    all_spider_ids = [spider.spider_id for spider in all_spiders]
#
#    for running_spider in running_spiders:
#        running_spider_id = running_spider[1]
#        if running_spider_id in all_spider_ids:
#            all_spider_ids.remove(running_spider_id)
#
#    logger.info(str(len(all_spider_ids)) + " spiders available at the moment: " + str(all_spider_ids))
#
#    all_spiders = [get_spider_object_for_given_spider_id(spider_id, all_spiders) for spider_id in all_spider_ids]
#    return all_spiders


#def get_spider_object_for_given_spider_id(spider_id, all_spiders):
#
#    for spider in all_spiders:
#        if spider.spider_id == spider_id:
#            target_spider = spider
#            break
#    return target_spider


def get_next_day(current_date):
    datetime_object = datetime.strptime(current_date, '%Y-%m-%d')
    datetime_object += timedelta(days=1)
    next_day = datetime_object.strftime("%Y-%m-%d")
    return next_day


if __name__ == "__main__" :
    logger.info("started")

    #all_spiders=get_spiders()
    #logger.info(str(all_spiders))
    #print(str(all_spiders))

    conn = dao.create_db_connection("/Users/emrecalisir/anaconda3/crawling.db")
    retry_wait_time = 600

    if not conn:
        logger.error("connection to sqlite could not be initialized, exiting")
        exit(-1)

    list_of_processed_days=[]
    year_start = 2016
    year_end = 2018
    month_start = 1
    month_end = 12
    day_start = 1
    day_end = 31

    MAX_CRAWLER_RUNNING = 3
    query = "brexit"
    # mongo listens in port 2017 by default
    client = MongoClient('localhost:27017')
    db = client.TweetScraper
    #subprocess.call('cd TweetScraper', shell=True)
    for year in range(year_start, year_end):
        for month in range(month_start, month_end):
            mymonth = str(month).rjust(2, '0')
            for day in range(day_start, day_end):
                myday = str(day).rjust(2, '0')
                target_date = str(year) + "-" + str(mymonth) + "-" + str(myday)
                try:
                    while (len(dao.get_running_crawlers(conn)) >= MAX_CRAWLER_RUNNING):
                        logger.info("All spiders are running, sleeping now, retry in " + str(retry_wait_time / 60) + " mins")
                        time.sleep(retry_wait_time)

                    if(dao.is_any_crawler_running_for_date(conn, target_date)):
                        logger.info("There is already a spider crawling for that date")
                        continue
                    else:
                        next_day = get_next_day(target_date)
                        part1 = "since:"+target_date
                        part2 = "until:"+next_day+"\""

                        process = subprocess.Popen(["scrapy", "crawl", "TweetScraper", "-a", "query=\""+query+"\"", "&"], cwd = "/Users/emrecalisir/PycharmProjects/ControlledCrawling/TweetScraper")
                        os_process_id = process.pid
                        if os_process_id is None:
                            logger.error("hey, where's the process Id?")
                        else:
                            logger.info("great: " + str(os_process_id))
                            dao.insert_to_db_new_crawler_process(conn, os_process_id, target_date)
                        time.sleep(5)

                except Exception as ex:
                    logger.error(str(ex))

