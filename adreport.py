from admanager.admanager import AdManager
from gmail_attachments.sendgrid_email import sendgridMail
from googlesheets.googlesheets import GoogleSheets

class adreport():

    def __init__(self):
        pass
    
    def run(self, *args, **kwargs):

        if args[0] is None:
            return None

        params = args[0]

        report, start_date, end_date = AdManager().run(params)
        
        if self.check_report(report):
            googlesheets = GoogleSheets(report, start_date, end_date)
            spreadsheet_url, new_trafficker_email = googlesheets.run(params)
            sendgridMail().send_successful_mail(params['order_id'], report, spreadsheet_url, new_trafficker_email)
        
        else:
            sendgridMail().send_fail_mail(params["order_id"], params['trafficker_email'])
   
    def check_report(self, report):
        
        if len(report) != 0:
            return True
        
        else:
            return False


        
        