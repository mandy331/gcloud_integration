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
        
        if not report.empty:
            googlesheets = GoogleSheets(report, start_date, end_date)
            googlesheets.run(params)
            spreadsheet_url, new_trafficker_email = googlesheets.run(params)
            #sendgridMail().send_successful_mail(params['order_id'], report, spreadsheet_url, new_trafficker_email)
        
        else:
            if 'traffickers' in params:
                sendgridMail().send_fail_mail(params["order_id"], params['traffickers'])



        
        