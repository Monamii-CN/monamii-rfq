import mysql.connector
import logging


class MydbOperator():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="RFQ_Reports"
    )

    tableName_prefix = "t_rfq_"
    tableName = ""

    def __init__(self, keyword):
        self.tableName = self.tableName_prefix + keyword

    def isEmptyTable(self):
        mycursor = self.mydb.cursor()
        sql = "SELECT COUNT(*) FROM " + self.tableName
        mycursor.execute(sql)
        return mycursor.fetchone() == 0

    def getByTitleAndBuyer(self, title, buyer):
        mycursor = self.mydb.cursor()
        sql = "SELECT * FROM " + self.tableName + " WHERE " + self.tableName + ".title='" + title + "'" + "and" + self.tableName + ".buyer='" + buyer + "'"
        mycursor.execute(sql)
        return mycursor.fetchone()

    def saveRFQ(self, rfqObj):
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
