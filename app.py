# -*- coding: utf-8 -*-
import argparse
import json
import sys

from admanager.admanager import AdManager
from gmail_attachments.sendgrid_email import sendgridMail
from adreport.adreport import adreport


class Empty():
    def run(self):
        print("No such service")
        pass

def factory(type, **kwargs):
    if type == 'adreport':
        return adreport()
    else:
        return Empty()

def main():
    parser = argparse.ArgumentParser(description='Google Service Integration')
    parser.add_argument('-s', '--service', help='enter google service')
    parser.add_argument('-p', '--params', help='custom parameters')
    args = parser.parse_args()

    app = factory(args.service)
    params = None
    if args.params:
        params = json.loads(args.params)
    else:
        with open('args.json', 'r') as f:
            params = json.load(f)

    app.run(params)

    sys.exit()


if __name__ == '__main__':
    main()