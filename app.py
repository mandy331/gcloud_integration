# -*- coding: utf-8 -*-
import argparse
import sys
from admanager.admanager import AdManager
from gmail_attachments.gmail_attachment import GmailAttachment

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
    args = parser.parse_args()
    
    app = factory(args.service)
    
    app.run()
    
    sys.exit()

if __name__ == '__main__':
    main()