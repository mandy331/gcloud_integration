# using SendGrid's Python Library
# https://github.com/sendgrid/sendgrid-python
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


class sendgridMail:

    def __init__(self):
        self.sender = "noreply@cw.com.tw"
    
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