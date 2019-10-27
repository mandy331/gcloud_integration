# -*- coding: utf-8 -*-
import argparse
import sys
from admanager.admanager import AdManager
from gmail_attachments.gmail_attachment import GmailAttachment
from googlesheets.googlesheets import GoogleSheets
import json
class Empty():
    def run(self):
        print("No such service")
        pass

def factory(type, **kwargs):
    if type == 'admanager':
        return AdManager()
    elif type == 'gmail':
        return GmailAttachment()
    else:
        return Empty()

def main():
    parser = argparse.ArgumentParser(description='Google Service Integration')
    parser.add_argument('-s', '--service', help='enter google service')
    parser.add_argument('-p', '--params', help='custom parameters')
    args = parser.parse_args()
    
    if args.service == "admanager":
        app = factory(args.service)
        params = None
        if args.params:
            params = json.loads(args.params)

            app.run(params)  
            report = app.run(params)
            googlesheets = GoogleSheets(report)
            googlesheets.run(params)
            
    else:
        app = factory(args.service)
        params = None
        if args.params:
            params = json.loads(args.params)
            
    #    app.run(params)
      
    sys.exit()

if __name__ == '__main__':
    main()