from datetime import date, datetime, timedelta
import gzip
import os
import sys
import tempfile
from os import environ as env
import re
import json

import pandas
from googleads import ad_manager, oauth2
from googleads import errors

from dotenv import load_dotenv

load_dotenv()


class AdManager():

    def __init__(self):
        self.googleads_yaml = env.get("GOOGLEADS_YAML", "googleads.yaml")
        pass

    def cert(self):

        return ad_manager.AdManagerClient.LoadFromStorage(self.googleads_yaml)

    def run(self, *args, **kwargs):

        if args[0] is None:
            return None

        params = args[0]

        if 'order_id' not in params:
            return None

        def check_future(str_date):
            if datetime.strptime(str_date, "%Y-%m-%d") > datetime.now():
                return date.today()
            else:
                return datetime.strptime(str_date, "%Y-%m-%d").date()
        
        if 'start_date' in params and 'end_date' in params:           
            start_date = check_future(params['start_date'])
            end_date = check_future(params['end_date'])            
            
        else:
            end_date = self.prior_week_end()
            start_date = self.prior_week_start()
    
        filename = self.download_order_report2(
            self.cert(), params['order_id'], start_date, end_date)

        report = self.advertisement_report(filename)
        
        return report, start_date, end_date
    
    def prior_week_end(self):
        return (datetime.now() - timedelta(days = ((datetime.now().isoweekday())%7))).date()

    def prior_week_start(self):
        return self.prior_week_end() - timedelta(days = 6)
    
    def download_order_report(self, client, order_id, start_date, end_date):
        # Initialize appropriate service.
        line_item_service = client.GetService(
            'LineItemService', version='v201908')
        # Initialize a DataDownloader.
        report_downloader = client.GetDataDownloader(version='v201908')

        # Filter for line items of a given order.
        statement = (ad_manager.StatementBuilder(version='v201911')
                     .Where('orderId = :orderId')
                     .WithBindVariable('orderId', int(order_id)))

        # Collect all line item custom field IDs for an order.
        custom_field_ids = set()

        # Get users by statement.
        while True:
            response = line_item_service.getLineItemsByStatement(
                statement.ToStatement())
            if 'results' in response and len(response['results']):
                print(response['results'][0])
                # Get custom field IDs from the line items of an order.
                for line_item in response['results']:
                    print(line_item['name'])
                    if 'customFieldValues' in line_item:
                        for custom_field_value in line_item['customFieldValues']:
                            custom_field_ids.add(
                                custom_field_value['customFieldId'])
                statement.offset += statement.limit
            else:
                break

        # Modify statement for reports
        statement.limit = None
        statement.offset = None
        statement.Where('ORDER_ID = :orderId')

        # Create report job.
        report_job = {
            'reportQuery': {
                'dimensions': ['LINE_ITEM_NAME', 'DATE', 'ORDER_NAME'],
                'dimensionAttributes': ['ORDER_TRAFFICKER'],
                'statement': statement.ToStatement(),
                'columns': ['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS'],
                'startDate': start_date,
                'endDate': end_date,
                'customFieldIds': list(custom_field_ids)
            }
        }

        try:
            # Run the report and wait for it to finish.
            report_job_id = report_downloader.WaitForReport(report_job)
        except errors.AdManagerReportError as e:
            print('Failed to generate report. Error was: %s' % e)

        # Change to your preferred export format.
        export_format = 'CSV_DUMP'

        report_file = tempfile.NamedTemporaryFile(
            suffix='.csv.gz', mode='wb', delete=False)
        print(report_file.name)
        # Download report data.
        report_downloader.DownloadReportToFile(
            report_job_id, export_format, report_file)

        # Display results.
        print('Report job with id "%s" downloaded to:\n%s' % (
            report_job_id, report_file.name))

        return report_file.name
    
    def download_order_report2(self, client, order_id, start_date, end_date):
        # Create statement object to filter for an order.
        statement = (ad_manager.StatementBuilder(version='v201911')
                    .Where('ORDER_ID = :id')
                    .WithBindVariable('id', int(order_id))
                    .Limit(None)  # No limit or offset for reports
                    .Offset(None))

        # Create report job.
        report_job = {
            'reportQuery': {
                'dimensions': ['LINE_ITEM_NAME', 'DATE', 'ORDER_NAME'],
                'dimensionAttributes': ['ORDER_TRAFFICKER'],
                'statement': statement.ToStatement(),
                'columns': ['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS'],
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': start_date,
                'endDate': end_date
            }
        }

        # Initialize a DataDownloader.
        report_downloader = client.GetDataDownloader(version='v201911')

        try:
            # Run the report and wait for it to finish.
            report_job_id = report_downloader.WaitForReport(report_job)
        except errors.AdManagerReportError as e:
            print('Failed to generate report. Error was: %s' % e)

        # Change to your preferred export format.
        export_format = 'CSV_DUMP'

        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)

        # Download report data.
        report_downloader.DownloadReportToFile(
            report_job_id, export_format, report_file)

        report_file.close()

        # Display results.
        print('Report job with id "%s" downloaded to:\n%s' % (
            report_job_id, report_file.name))
        
        return report_file.name

    def advertisement_report(self, report_file_name):
        
        # 讀取檔案
        report = pandas.read_csv(str(report_file_name), compression='gzip', error_bad_lines=False)
        
        if report.empty:
            return report
        
        else: 
            # 正則式 讀取版位、活動
            ITEM = report["Dimension.LINE_ITEM_NAME"]
            pattern = r"\[(.*)\](.*)"
            advertisement, campaign = [], []
            for text in ITEM:
                if text[0] == "[":
                    result = re.findall(pattern, text)
                    campaign.append(result[0][0])
                    advertisement.append(result[0][1].strip())
                else:
                    campaign.append("成效報表")
                    advertisement.append(text)

            report["版位名稱"], report["Campaign"] = advertisement, campaign
            
            # 轉換Dimension.DATE格式
            report["Dimension.DATE"] = pandas.to_datetime(report["Dimension.DATE"])
            report["Dimension.DATE"] = report["Dimension.DATE"].apply(lambda x: x.date())

            new_report = report[["Dimension.ORDER_NAME", "Dimension.DATE", "版位名稱", "Campaign",
                                "Column.AD_SERVER_IMPRESSIONS", "Column.AD_SERVER_CLICKS", "DimensionAttribute.ORDER_TRAFFICKER"]]

            return new_report
