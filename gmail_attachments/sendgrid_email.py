# using SendGrid's Python Library
# https://github.com/sendgrid/sendgrid-python
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from pprint import pprint
import pandas
import datetime
from dateutil.relativedelta import relativedelta
import re


class sendgridMail:
    
    def __init__(self):
        self.sender = "noreply@cw.com.tw"
    
    def send_successful_mail(self, order_id, report, spreadsheet_url, new_trafficker_email):
        
        # 產出狀態
        condition = "成功"
        
        # 客戶名稱
        customer_name = str(report["Dimension.ORDER_NAME"][0])
        
        # 客戶ID
        customer_id = str(order_id)

        # 報表產生時間
        now = datetime.datetime.now()
        period_now = now.strftime('%Y/%m/%d %H:%M') 

        # 報表抓取數據時間區間 
        report["Dimension.DATE"] = pandas.to_datetime(report["Dimension.DATE"])
        early_day = report["Dimension.DATE"].min()
        last_day = report["Dimension.DATE"].max()
        period_start = early_day.strftime('%Y/%m/%d %H:%M') 
        period_end = last_day.strftime('%Y/%m/%d %H:%M')
        period_time = period_start + " - " + period_end
        
        # 報表連結
        spreadsheet_url = str(spreadsheet_url)

        # email格式
        period_now_subject_format = str(now.year) + str(now.month) + str(now.day)
        email_subject = "[成效報表]{}_{}更新{}"
        email_subject = email_subject.format(customer_name, period_now_subject_format, condition)
        
        for i in range(len(new_trafficker_email)):
            trafficker_name = str("閔慈")
            trafficker_email = str("mhuang98331@gmail.com")
            #trafficker_name = str(new_trafficker_email["負責人"][i])
            #trafficker_email = str(new_trafficker_email["Email"][i])
            email_text_body = "Dear{}：<br><br>    以下為{}的成效報表資訊：<br><br>    產出狀態：{}<br>    客戶ID：{}<br>    報表產生時間：{}<br>    報表抓取數據時間區間：{}<br>    報表連結：{}<br><br>Best Regards,<br>CW Robot"
            email_text_body = email_text_body.format(trafficker_name, customer_name, condition, customer_id, period_now, period_time, spreadsheet_url)           
            self.send(trafficker_email, email_subject, email_text_body) 


    def send_fail_mail(self, order_id, trafficker_email):
        
        # 產出狀態
        condition = "失敗"
                
        # 客戶ID
        customer_id = str(order_id)

        # 報表產生時間
        now = datetime.datetime.now()
        period_now = now.strftime('%Y/%m/%d %H:%M') 

        # email格式
        period_now_subject_format = str(now.year) + str(now.month) + str(now.day)
        email_subject = str("[成效報表]" + customer_id + "_" + period_now_subject_format + "_" + "更新" + condition)
        
        # 預定要寄給的負責人
        trafficker_name, email = [], []
        for j in trafficker_email:
            trafficker_name = j.get("name")
            email = j.get("email")
            email_text_body = "Dear{}：<br><br>    產出狀態：{}<br>    客戶ID：{}<br>    報表產生時間：{}<br><br>Best Regards,<br>CW Robot"
            email_text_body = email_text_body.format(trafficker_name, condition, customer_id, period_now)
            self.send(email, email_subject, email_text_body)

    def send(self, recepient, subject, text_body):
        
        message = Mail(
            from_email=self.sender,
            to_emails=[recepient],
            subject=subject,
            html_content=text_body)
        try:
            sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
            response = sg.send(message)
            print(response.status_code)
            print(response.body)
            print(response.headers)
        except Exception as e:
            print(e)