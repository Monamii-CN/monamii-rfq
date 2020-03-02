import mysql.connector
import logging


class MydbOperator:
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="rfq_reports"
    )

    tableName_prefix = "t_rfq_"
    tableName = ""

    def __init__(self, keyword):
        self.tableName = self.tableName_prefix + keyword

    def is_empty_table(self):

        mycursor = self.mydb.cursor()
        sql = "SELECT COUNT(*) FROM " + self.tableName
        mycursor.execute(sql)
        return mycursor.fetchone() == 0

    def create_table(self):
        mycursor = self.mydb.cursor()
        sql = "CREATE TABLE IF NOT EXISTS rfq_reports." + self.tableName + "( id BIGINT NOT NULL AUTO_INCREMENT , title VARCHAR(255) NOT NULL DEFAULT '\"\"' ,  quantity INT NOT NULL DEFAULT '0' ,  unit VARCHAR(20) NOT NULL DEFAULT '\"\"' ,  stars INT(10) NOT NULL ,  open_time VARCHAR(50) NOT NULL DEFAULT '\"\"' ,  origin VARCHAR(50) NOT NULL DEFAULT '\"\"' ,  buyer VARCHAR(255) NOT NULL DEFAULT '\"\"' ,  buyer_tag VARCHAR(255) NOT NULL DEFAULT '\"\"' ,  quote_left INT(100) NOT NULL DEFAULT '0' ,  description VARCHAR(255) NOT NULL DEFAULT '\"\"' ,  link VARCHAR(255) NOT NULL DEFAULT '\"\"' ,  PRIMARY KEY  (id)) ENGINE = InnoDB;"
        mycursor.execute(sql)

    def get_by_title_and_buyer(self, title, buyer):
        mycursor = self.mydb.cursor()
        sql = "SELECT * FROM " + self.tableName + " WHERE " + self.tableName + ".title='" + title + "'" + "and" + self.tableName + ".buyer='" + buyer + "'"
        mycursor.execute(sql)
        return mycursor.fetchone()

    def save_rfq(self, rfqObj):
        mycursor = self.mydb.cursor()
        logging.info("Save RFQ Record: " + rfqObj.rfq_title)

        sql = "INSERT INTO " + self.tableName + " (rfq_title, rfq_quantity, rfq_unit, rfq_stars, rfq_open_time, rfq_origin, rfq_buyer, rfq_buyer_tag, rfq_quote, rfq_desc, rfq_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (rfqObj.rfq_title, rfqObj.rfq_quantity, rfqObj.rfq_unit, rfqObj.rfq_stars, rfqObj.rfq_open_time,
               rfqObj.rfq_origin, rfqObj.rfq_buyer, rfqObj.rfq_buyer_tag, rfqObj.rfq_quote, rfqObj.rfq_desc,
               rfqObj.rfq_link)

        try:
            mycursor.execute(sql, val)
            self.mydb.commit()
        except:
            logging.error("Save RFQ Record: " + rfqObj.rfq_title + " fail")
            self.mydb.rollback()

    def close(self):
        self.mydb.close()
        print("DB connection closed")
