import logging

import MySQLdb


class MydbOperator:
    mydb = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="",
        database="rfq_reports",
        use_unicode=True,
        charset="utf8"
    )

    tableName_prefix = "t_rfq_"

    def __init__(self, keyword):
        self.tableName = self.tableName_prefix + keyword

    def is_empty_table(self):
        mycursor = self.mydb.cursor()
        sql = "SELECT COUNT(*) FROM " + self.tableName
        mycursor.execute(sql)
        return mycursor.fetchone() == 0

    def create_table(self):
        mycursor = self.mydb.cursor()
        sql = "CREATE TABLE IF NOT EXISTS rfq_reports." + self.tableName + "(id bigint(20) NOT NULL AUTO_INCREMENT ,title varchar(255) NOT NULL DEFAULT '""',quantity int(11) DEFAULT '0',unit varchar(20) DEFAULT '""',stars int(10) DEFAULT NULL,open_time varchar(50) DEFAULT '""',origin varchar(50) NOT NULL DEFAULT '""',buyer varchar(255) NOT NULL DEFAULT '""',buyer_tag varchar(255) DEFAULT '""',quote_left int(100) DEFAULT '0',description varchar(255) DEFAULT '""',link varchar(255) NOT NULL DEFAULT '""',time_created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP , PRIMARY KEY  (id)) ENGINE = InnoDB;"
        mycursor.execute(sql)

    def get_by_title_and_buyer(self, title, buyer):
        mycursor = self.mydb.cursor()
        sql = "SELECT * FROM " + self.tableName + " WHERE " + self.tableName + ".title='" + title + "'" + " and " + self.tableName + ".buyer='" + buyer + "'"
        mycursor.execute(sql)
        return mycursor.fetchone()

    def save_rfq(self, rfq_object):
        mycursor = self.mydb.cursor()
        logging.info("Save RFQ Record: " + rfq_object.title)

        sql = "INSERT INTO " + self.tableName + " (title, quantity, unit, stars, open_time, origin, buyer, buyer_tag, quote_left, description, link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (rfq_object.title, rfq_object.quantity, rfq_object.unit, rfq_object.stars, rfq_object.open_time,
               rfq_object.origin, rfq_object.buyer, rfq_object.buyer_tag, rfq_object.quote_left, rfq_object.description,
               rfq_object.link)
        try:
            mycursor.execute(sql, val)
            self.mydb.commit()
        except MySQLdb.Error as err:
            print("Something went wrong: {}".format(err))
            logging.error("Save RFQ Record: " + rfq_object.title + " fail")
            self.mydb.rollback()

    def close(self):
        self.mydb.close()
        print("DB connection closed")
