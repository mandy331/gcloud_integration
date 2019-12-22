import datetime
import gzip
import os
import sys
import tempfile
from os import environ as env
import re
import json

import pandas
from googleads import ad_manager, oauth2

from dotenv import load_dotenv
from googlesheets.googlesheets import GoogleSheets

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

        if 'start_date' in params and 'end_date' in params:
            start_date = datetime.datetime.strptime(
                params['start_date'], "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(
                params['end_date'], "%Y-%m-%d").date()
        else:
            today = datetime.date.today()
            end_date = today + datetime.timedelta(6 - today.weekday())
            start_date = end_date - datetime.timedelta(days=6)
        
        filename = self.download_order_report(
            self.cert(), params['order_id'], start_date, end_date)

        report = self.advertisement_report(filename)
        googlesheets = GoogleSheets(report, end_date)
        googlesheets.run(params)

    def print_all_orders(self, ad_manager_client):

        # Initialize appropriate service.
        order_service = ad_manager_client.GetService(
            'OrderService', version='v201908')

        # Create a statement to select orders.
        statement = ad_manager.StatementBuilder(version='v201908')

        # Retrieve a small amount of orders at a time, paging
        # through until all orders have been retrieved.
        while True:
            response = order_service.getOrdersByStatement(
                statement.ToStatement())
            if 'results' in response and len(response['results']):
                for order in response['results']:
                    # Print out some information for each order.
                    print('Order with ID "%d" and name "%s" was found.\n' % (order['id'],
                                                                             order['name']))
                statement.offset += statement.limit
            else:
                break

        print('\nNumber of results found: %s' % response['totalResultSetSize'])

    def download_order_report(self, client, order_id, start_date, end_date):
        # Initialize appropriate service.
        line_item_service = client.GetService(
            'LineItemService', version='v201908')
        # Initialize a DataDownloader.
        report_downloader = client.GetDataDownloader(version='v201908')

        # Filter for line items of a given order.
        statement = (ad_manager.StatementBuilder(version='v201908')
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
                'dimensions': ['LINE_ITEM_NAME', 'DATE', 'ORDER_NAME', 'CREATIVE_SIZE','AD_UNIT_NAME'],
                'dimensionAttributes': ['ORDER_TRAFFICKER'],
                'statement': statement.ToStatement(),
                'columns': ['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS', 'AD_SERVER_CTR'],
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

    def advertisement_report(self, report_file):
        # 讀取報表
        report = pandas.read_csv(str(report_file), compression='gzip')

        # 正則式 讀取版位、活動
        ITEM = report["Dimension.LINE_ITEM_NAME"]
        pattern = r"(([[])(.*)([]])|.*)(.*)"
        campaign = []
        for text in ITEM:
            if text[0] == "[":
                result = re.findall(pattern, text)
                campaign.append(result[0][2])
            else:
                campaign.append("成效報表")
        report["Campaign"] = campaign

        # 版位名稱
        report["版位名稱"] =  report["Dimension.AD_UNIT_NAME"] + "\n" + report["Dimension.CREATIVE_SIZE"]
        
        # 轉換Dimension.DATE格式
        report["Dimension.DATE"] = pandas.to_datetime(report["Dimension.DATE"])

        new_report = report[["Dimension.ORDER_NAME", "Dimension.DATE", "版位名稱", "Campaign",
                                       "Column.AD_SERVER_IMPRESSIONS", "Column.AD_SERVER_CLICKS", "Column.AD_SERVER_CTR","DimensionAttribute.ORDER_TRAFFICKER"]]
        
        return new_report