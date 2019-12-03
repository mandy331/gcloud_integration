from __future__ import print_function
import pickle
import os.path
import oauth2client
from googleapiclient import discovery
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from oauth2client.service_account import ServiceAccountCredentials
from os import environ as env
import time

from pprint import pprint
import pandas
import datetime
import re

from gmail_attachments.sendgrid_email import sendgridMail

# If modifying these scopes, delete the file token.pickle.
SHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly',
                 'https://www.googleapis.com/auth/spreadsheets']
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/drive.file']
# The ID and range of a sample spreadsheet.
# SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
# SAMPLE_RANGE_NAME = 'Class Data!A2:E'


class GoogleSheets:

    def __init__(self, report):
        self.key = 'cert/cw-web-prod-ad-manager.json'
        self.service = None
        self.folder = '1r0dr5jDA9FRvjhoEC8YMNdYUJN_YM8_B'
        self.report = report
        self.sendgridMail = sendgridMail()
        self.start_row = 8
        pass

    def cert(self, SCOPES):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.key, SCOPES)
        if SCOPES == SHEETS_SCOPES:
            self.service = build('sheets', 'v4', credentials=credentials)
        elif SCOPES == DRIVE_SCOPES:
            delegated_credentials = credentials.create_delegated(
                'robot@cw.com.tw')
            self.service = build('drive', 'v3', credentials=credentials)

    def run(self, *args, **kwargs):

        self.cert(SHEETS_SCOPES)

        if args[0] is None:
            return None

        params = args[0]

        if 'template_spreadsheet_id' not in params:
            return None

        if 'template_sheet_id' not in params:
            return None

        if 'placementmap_spreadsheet_id' not in params:
            return None

        if 'spreadsheet_name' not in params:
            return None

        if len(self.report) == 0:
            self.send_fail_mail(
                params["order_id"], params["spreadsheet_name"], params["trafficker_email"])

        else:
            placementmap_df = self.get_placementmap_values(
                params['placementmap_spreadsheet_id'])
            create_spreadsheet_id = self.create_spreadsheet(
                params['spreadsheet_name'])
            campaign, campaign_numbers = self.count_campaign(self.report)

            for i in range(campaign_numbers):
                # 創建sheet
                sheet_id = self.copy_template_to_sheets(
                    params["template_spreadsheet_id"], params["template_sheet_id"], create_spreadsheet_id)
                self.rename_sheet(create_spreadsheet_id, sheet_id, campaign[i])

                # 填入版位名稱、走期
                all_data, update_data1 = self.merge_whole_data(
                    placementmap_df, self.report, campaign[i])
                self.update_values(create_spreadsheet_id, update_data1)

                # 填入日期、數據、總和
                early_day, days, months = self.count_months(
                    self.report, campaign[i])
                for k in range(months):
                    update_data2, last_column_index, last_row_index, last_column_index = self.merge_month_data(
                        placementmap_df, self.report, campaign[i], early_day, k, days, self.start_row)
                    self.copy_total_three_rows(
                        create_spreadsheet_id, sheet_id, last_row_index)
                    self.update_values(create_spreadsheet_id, update_data2)
                self.delete_empty_cols_rows(
                    create_spreadsheet_id, sheet_id, last_column_index, last_row_index)
                self.start_row = 8

            self.delete_first_sheet(create_spreadsheet_id)
            create_spreadsheet_url = self.get_url(create_spreadsheet_id)

            self.cert(DRIVE_SCOPES)
            self.move_to_folder(self.folder, create_spreadsheet_id)
            self.send_successful_mail(
                params["order_id"], self.report, create_spreadsheet_url, params["trafficker_email"])

    def get_placementmap_values(self, placement_map_id):

        spreadsheet_id = placement_map_id  # TODO: Update placeholder value.

        # The A1 notation of the values to retrieve.
        range_ = '工作表1'  # TODO: Update placeholder value.

        majorDimension = 'COLUMN'

        # How values should be represented in the output.
        # The default render option is ValueRenderOption.FORMATTED_VALUE.
        # TODO: Update placeholder value.
        value_render_option = 'FORMATTED_VALUE'

        # How dates, times, and durations should be represented in the output.
        # This is ignored if value_render_option is
        # FORMATTED_VALUE.
        # The default dateTime render option is [DateTimeRenderOption.SERIAL_NUMBER].
        # TODO: Update placeholder value.
        date_time_render_option = 'SERIAL_NUMBER'

        request = self.service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_,
                                                           valueRenderOption=value_render_option, dateTimeRenderOption=date_time_render_option)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        placementmap_df = pandas.DataFrame(response["values"][1:], columns=[
                                           "版位名稱", "委刊項對照名稱"]).reset_index(drop=True)

        return placementmap_df

    def create_spreadsheet(self, spreadsheet_name):

        spreadsheet = {
            'properties': {
                'title': spreadsheet_name
            }
        }
        spreadsheet = self.service.spreadsheets().create(body=spreadsheet,
                                                         fields='spreadsheetId')
        response = spreadsheet.execute()

        spreadsheet_id = response.get("spreadsheetId", "")

        return spreadsheet_id

    def count_campaign(self, report):

        campaign = list(set(report["CAMPAIGN"]))
        campaign_numbers = len(campaign)
        print(campaign)
        return campaign, campaign_numbers

    def copy_template_to_sheets(self, template_spreadsheet_id, template_sheet_id, create_spreadsheet_id):

        # TODO: Update placeholder value.
        spreadsheet_id = template_spreadsheet_id

        # The ID of the sheet to copy.
        sheet_id = template_sheet_id  # TODO: Update placeholder value.

        copy_sheet_to_another_spreadsheet_request_body = {
            # The ID of the spreadsheet to copy the sheet to.
            'destination_spreadsheet_id': create_spreadsheet_id,
        }

        request = self.service.spreadsheets().sheets().copyTo(spreadsheetId=spreadsheet_id,
                                                              sheetId=sheet_id, body=copy_sheet_to_another_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response["sheetId"])

        sheet_id = response.get("sheetId", "")

        return sheet_id

    def define_sheet_range(self, spreadsheet_id):
        spreadsheet_id = 'my-spreadsheet-id'  # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            # A list of updates to apply to the spreadsheet.
            # Requests will be applied in the order they are specified.
            # If any request is not valid, no requests will be applied.
            'requests': [],  # TODO: Update placeholder value.

            # TODO: Add desired entries to the request body.
        }

        request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                     body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)

    def delete_first_sheet(self, spreadsheet_id):

        spreadsheet_id = spreadsheet_id  # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            # A list of updates to apply to the spreadsheet.
            # Requests will be applied in the order they are specified.
            # If any request is not valid, no requests will be applied.
            'requests': [
                {
                    "deleteSheet": {
                        "sheetId": 0  # 預設刪掉第一個空的sheet，不知道這樣可不可以
                    }
                },
            ],
        }

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                          body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)

    def rename_sheet(self, spreadsheet_id, sheet_id, campaign_name):

        spreadsheet_id = spreadsheet_id  # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            # A list of updates to apply to the spreadsheet.
            # Requests will be applied in the order they are specified.
            # If any request is not valid, no requests will be applied.
            'requests': [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "title": campaign_name,
                        },
                        "fields": "title"
                    }
                }
            ],
        }

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                          body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)

    def merge_month_data(self, placementmap_df, advertisement_report, compaign_name, early_day, k, days, start_row):

        # 篩選不同的報表
        advertisement_report = advertisement_report[advertisement_report["CAMPAIGN"] == compaign_name]

        # 轉換Dimension.DATE格式
        advertisement_report["Dimension.DATE"] = pandas.to_datetime(
            advertisement_report["Dimension.DATE"])

        # 篩選目前的月份
        cur_month = early_day.month + k

        # 整理日期數據
        Date, Date_Format = [], []
        day = early_day
        for i in range(days+1):
            new_day = day + datetime.timedelta(days=i)
            if new_day.month == cur_month:
                Date.append(new_day)
                Date_Format.append(str(new_day.month) + "/" + str(new_day.day))
        Date_df = pandas.DataFrame(
            columns=["Dimension.DATE", "Date_Format", "Index"])
        Date_df["Dimension.DATE"], Date_df["Date_Format"] = Date, Date_Format
        Date_df["Index"] = [i + start_row for i in range(len(Date_df))]

        # Total起始欄
        last_row_index = Date_df["Index"].max() + 1

        # 更新往下起始的Row數
        self.start_row = last_row_index + 3

        # 合併 成效報表、版位名稱對照表、日期
        all_data = pandas.merge(
            advertisement_report, placementmap_df, left_on="PLACEMENT", right_on="委刊項對照名稱")
        all_data = pandas.merge(all_data, Date_df, on="Dimension.DATE")

        # 整理版位和填入的版位名稱
        placement_list = sorted(list(set(all_data["版位名稱"])))
        Column1, Column2, Column3, Column1_index = [], [], [], []
        last_column_index = 0
        for i in range(len(placement_list)):
            Column1_index.append(i)
            if i <= 7:
                Column1.append(chr(65+i*3+1))
                Column2.append(chr(65+i*3+2))
                Column3.append(chr(65+i*3+3))
            elif i == 8:
                k = i - 8
                Column1.append(chr(65+i*3+1))  # Z
                Column2.append(chr(65) + chr(64+i*3+1))  # AA
                Column3.append(chr(65) + chr(64+i*3+2))  # AB
            else:
                j = i - 9
                Column1.append(chr(65) + chr(66+i*3+1))
                Column2.append(chr(65) + chr(66+i*3+2))
                Column3.append(chr(65) + chr(66+i*3+3))
            last_column_index = 1 + (i+1)*3

        placement_index_df = zip(
            placement_list, Column1, Column2, Column3, Column1_index)
        placement_index_df = pandas.DataFrame(placement_index_df, columns=[
                                              "版位名稱", "Column1", "Column2", "Column3", "Column1_index"])

        # 和版位名稱和版位index合併
        all_data = pandas.merge(all_data, placement_index_df, on="版位名稱")

        # 將數據轉為可放入googlesheets的格式
        update_data2 = []

        # 整理填入數據
        clean_data = pandas.DataFrame(all_data.groupby(['Column1', 'Column2', 'Column3', 'Index'])[
                                      'Column.AD_SERVER_IMPRESSIONS', 'Column.AD_SERVER_CLICKS'].sum().reset_index(drop=False))
        clean_data["Column.AD_SERVER_CTR"] = round(
            clean_data["Column.AD_SERVER_CLICKS"] / clean_data["Column.AD_SERVER_IMPRESSIONS"], 4)
        for j in range(len(clean_data)):
            values = []
            values.append([int(clean_data["Column.AD_SERVER_IMPRESSIONS"][j]), int(
                clean_data["Column.AD_SERVER_CLICKS"][j]), int(clean_data["Column.AD_SERVER_CTR"][j])])
            data = {
                "range": compaign_name + "!" + str(clean_data["Column1"][j]) + str(clean_data["Index"][j]) + ":" + str(clean_data["Column3"][j]) + str(clean_data["Index"][j]),
                "majorDimension": 'ROWS',
                "values": values,
            }
            update_data2.append(data)

        # 整理Total三欄數據
        unique_column = list(set(clean_data["Column1"]))
        for j in range(len(unique_column)):
            df = clean_data[clean_data["Column1"] ==
                            unique_column[j]].reset_index(drop=True)
            print("==================")
            print(df)
            data = {
                "range": compaign_name + "!" + str(df["Column1"][0]) + str(last_row_index) + ":" + str(df["Column2"][0]) + str(last_row_index),
                "majorDimension": 'ROWS',
                "values":
                [[int(sum(df["Column.AD_SERVER_IMPRESSIONS"])),
                  int(sum(df["Column.AD_SERVER_CLICKS"]))]],
            }
            print(str(df["Column1"][0]) + str(last_row_index) +
                  ":" + str(df["Column2"][0]) + str(last_row_index))
            print(int(sum(df["Column.AD_SERVER_IMPRESSIONS"])))
            print(int(sum(df["Column.AD_SERVER_CLICKS"])))
            print("==================")
            update_data2.append(data)

        # 將日期填入googlesheets
        date = []
        for k in range(len(Date_df)):
            date.append([str(Date_df["Date_Format"][k])])
        data = {
            "range": compaign_name + "!" + "A8:A",
            'majorDimension': 'ROWS',
            "values": date,
        }
        update_data2.append(data)

        return update_data2, last_column_index, last_row_index, last_column_index

    def count_months(self, advertisement_report, compaign_name):

        # 篩選不同的報表
        advertisement_report = advertisement_report[advertisement_report["CAMPAIGN"] == compaign_name]

        # 轉換Dimension.DATE格式
        advertisement_report["Dimension.DATE"] = pandas.to_datetime(
            advertisement_report["Dimension.DATE"])

        # 算委刊項中最早和最晚日期
        early_day = advertisement_report["Dimension.DATE"].min()
        last_day = advertisement_report["Dimension.DATE"].max()
        days = (last_day - early_day).days
        months = early_day.month - last_day.month
        months = months + 1

        return early_day, days, months

    def merge_whole_data(self, placementmap_df, advertisement_report, compaign_name):

        # 篩選不同的報表
        advertisement_report = advertisement_report[advertisement_report["CAMPAIGN"] == compaign_name]

        # 轉換Dimension.DATE格式
        advertisement_report["Dimension.DATE"] = pandas.to_datetime(
            advertisement_report["Dimension.DATE"])

        # 算委刊項中最早和最晚日期
        early_day = advertisement_report["Dimension.DATE"].min()
        last_day = advertisement_report["Dimension.DATE"].max()
        days = (last_day - early_day).days

        # 整理日期數據
        Date, Date_Format = [], []
        day = early_day
        for i in range(days+1):
            new_day = day + datetime.timedelta(days=i)
            Date.append(new_day)
            Date_Format.append(str(new_day.month) + "/" + str(new_day.day))
        Date_df = pandas.DataFrame(
            columns=["Dimension.DATE", "Date_Format", "Index"])
        Date_df["Dimension.DATE"], Date_df["Date_Format"] = Date, Date_Format
        Date_df["Index"] = [i + 8 for i in range(len(Date_df))]

        # 合併 成效報表、版位名稱對照表、日期
        all_data = pandas.merge(
            advertisement_report, placementmap_df, left_on="PLACEMENT", right_on="委刊項對照名稱")
        all_data = pandas.merge(all_data, Date_df, on="Dimension.DATE")

        # 整理版位和填入的版位名稱
        placement_list = sorted(list(set(all_data["版位名稱"])))
        Column1, Column2, Column3, Column1_index = [], [], [], []
        last_column_index = 0
        for i in range(len(placement_list)):
            Column1_index.append(i)
            if i <= 7:
                Column1.append(chr(65+i*3+1))
                Column2.append(chr(65+i*3+2))
                Column3.append(chr(65+i*3+3))
            elif i == 8:
                k = i - 8
                Column1.append(chr(65+i*3+1))  # Z
                Column2.append(chr(65) + chr(64+i*3+1))  # AA
                Column3.append(chr(65) + chr(64+i*3+2))  # AB
            else:
                j = i - 9
                Column1.append(chr(65) + chr(66+i*3+1))
                Column2.append(chr(65) + chr(66+i*3+2))
                Column3.append(chr(65) + chr(66+i*3+3))
            last_column_index = 1 + (i+1)*3

        placement_index_df = zip(
            placement_list, Column1, Column2, Column3, Column1_index)
        placement_index_df = pandas.DataFrame(placement_index_df, columns=[
                                              "版位名稱", "Column1", "Column2", "Column3", "Column1_index"])

        # 和版位名稱和版位index合併
        all_data = pandas.merge(all_data, placement_index_df, on="版位名稱")

        # 整理各版位的走期
        unique_placement = list(set(all_data["Column1"]))
        period = []
        for i in range(len(unique_placement)):
            period_list = list(
                set(all_data[all_data["Column1"] == unique_placement[i]]["Dimension.DATE"]))
            start_day = min(period_list)
            end_day = max(period_list)
            days = (end_day - start_day).days
            if len(period_list) == days + 1:
                period.append(str(start_day.month)+"/"+str(start_day.day) +
                              "-"+str(end_day.month)+"/"+str(end_day.day))
            else:
                many_period = ""
                for i in range(days - 1):
                    current_day = start_day + datetime.timedelta(days=i)
                    if current_day not in period_list:
                        end_day = current_day - datetime.timedelta(days=1)
                        if end_day == start_day:
                            many_period += str(start_day.month) + \
                                "/" + str(start_day.day)
                        else:
                            many_period += str(start_day.month) + "/" + str(start_day.day) + "-" + str(
                                end_day.month) + "/" + str(end_day.day) + "+"
                        start_day = current_day + datetime.timedelta(days=1)
                    else:
                        pass
                many_period = many_period.rstrip('+')
                period.append(many_period)

        period_df = zip(unique_placement, period)
        period_df = pandas.DataFrame(period_df, columns=["Column1", "Period"])

        # 將數據轉為可放入googlesheets的格式
        update_data1 = []

        # 將走期填入googlesheets
        for x in range(len(period_df)):
            data = {
                "range": compaign_name + "!" + str(period_df["Column1"][x]) + "6",
                'majorDimension': 'ROWS',
                "values": [[period_df["Period"][x]]],
            }
            update_data1.append(data)

        # 將版位名稱填入googlesheets
        for x in range(len(placement_index_df)):
            data = {
                "range": compaign_name + "!" + str(placement_index_df["Column1"][x]) + "4",
                'majorDimension': 'ROWS',
                "values": [[placement_index_df["版位名稱"][x]]],
            }
            update_data1.append(data)

        return all_data, update_data1

    def update_values(self, spreadsheet_id, update_data):

        spreadsheet_id = spreadsheet_id  # TODO: Update placeholder value.

        batch_update_values_request_body = {
            # How the input data should be interpreted.
            'value_input_option': 'RAW',  # TODO: Update placeholder value.
            # The new values to apply to the spreadsheet.
            # append進去
            'data': update_data,

        }

        request = self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body=batch_update_values_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)

    def copy_total_three_rows(self, spreadsheet_id, sheet_id, last_row_index):

        spreadsheet_id = spreadsheet_id  # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            "requests": [
                {
                    "cutPaste": {
                        "source": {
                            "sheetId": sheet_id,
                            "startRowIndex": 299,
                            "endRowIndex": 302,
                            "startColumnIndex": 0,
                            "endColumnIndex": 37,
                        },
                        "destination": {
                            "sheetId": sheet_id,
                            "rowIndex": int(last_row_index) - 1,
                            "columnIndex": 0
                        },
                        "pasteType": "PASTE_NORMAL",
                    }
                }
            ]
        }

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                          body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)

    def delete_empty_cols_rows(self, spreadsheet_id, sheet_id, last_column_index, last_row_index):

        spreadsheet_id = spreadsheet_id  # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            'requests': [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": int(last_column_index),
                            "endIndex": 37
                        }
                    }
                },
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": int(last_row_index) + 2,
                            "endIndex": 797
                        }
                    }
                },

            ],

        }

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                          body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)

    def get_url(self, spreadsheet_id):

        spreadsheet_id = spreadsheet_id  # TODO: Update placeholder value.

        # The ranges to retrieve from the spreadsheet.
        ranges = []  # TODO: Update placeholder value.

        # True if grid data should be returned.
        # This parameter is ignored if a field mask was set in the request.
        include_grid_data = False  # TODO: Update placeholder value.

        request = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id,
                                                  ranges=ranges, includeGridData=include_grid_data)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response.get("spreadsheetUrl"))
        spreadsheet_url = response.get("spreadsheetUrl")
        return spreadsheet_url

    def move_to_folder(self, folder_id, spreadsheet_id):

        folder_id = folder_id
        file_id = spreadsheet_id
        # Retrieve the existing parents to remove
        file = self.service.files().get(fileId=file_id,
                                        fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        # Move the file to the new folder
        file = self.service.files().update(fileId=file_id,
                                           addParents=folder_id,
                                           removeParents=previous_parents,
                                           supportsAllDrives=True,
                                           fields='id, parents').execute()

    def send_successful_mail(self, order_id, advertisement_report, spreadsheet_url, trafficker_email):

        # 產出狀態
        condition = "成功"

        # 客戶名稱
        customer_name = str(advertisement_report["Dimension.ORDER_NAME"][0])

        # 客戶ID
        customer_id = str(order_id)

        # 報表產生時間
        now = datetime.datetime.now()
        period_now = now.strftime('%Y/%m/%d %H:%M')

        # 報表抓取數據時間區間
        advertisement_report["Dimension.DATE"] = pandas.to_datetime(
            advertisement_report["Dimension.DATE"])
        early_day = advertisement_report["Dimension.DATE"].min()
        last_day = advertisement_report["Dimension.DATE"].max()
        period_start = early_day.strftime('%Y/%m/%d %H:%M')
        period_end = last_day.strftime('%Y/%m/%d %H:%M')
        period_time = period_start + " - " + period_end

        # 報表連結
        spreadsheet_url = str(spreadsheet_url)

        # email格式
        period_now_subject_format = str(
            now.year) + str(now.month) + str(now.day)
        email_subject = str("[成效報表]" + customer_name + "_" +
                            period_now_subject_format + "_" + "更新" + condition)

        # 預定要寄給的負責人
        trafficker_name, email = [], []
        for j in trafficker_email:
            trafficker_name.append(j.get("name"))
            email.append(j.get("email"))

        # 負責人姓名和負責人信箱
        trafficker = advertisement_report["DimensionAttribute.ORDER_TRAFFICKER"]
        pattern = r"(.*)(\s)[(](.*)[)]"
        for person in trafficker:
            result = re.findall(pattern, person)
            trafficker_name.append(result[0][0])
            email.append(result[0][2])

        tra = zip(trafficker_name, email)
        tra_df = pandas.DataFrame(
            tra, columns=["負責人", "Email"]).drop_duplicates().reset_index(drop=True)

        for i in range(len(tra_df)):
            trafficker_name = str(tra_df["負責人"][i])
            trafficker_email = str(tra_df["Email"][i])
            email_text_body = "Dear" + trafficker_name + "<br>    以下為" + customer_name + "的成效報表資訊：" + "<br><br>    產出狀態：" + condition + "<br>    客戶ID：" + customer_id + \
                "<br>    報表產生時間：" + period_now + "<br>    報表抓取數據時間區間：" + period_time + \
                "<br>    報表連結：" + spreadsheet_url + "<br><br>Best Regards,<br>CW Robot"
            self.sendgridMail.send(
                trafficker_email, email_subject, email_text_body)

    def send_fail_mail(self, order_id, spreadsheet_name, trafficker_email):

        # 產出狀態
        condition = "失敗"

        # 原預定產出報表名稱
        spreadsheet_name = spreadsheet_name

        # 客戶ID
        customer_id = str(order_id)

        # 報表產生時間
        now = datetime.datetime.now()
        period_now = now.strftime('%Y/%m/%d %H:%M')

        # email格式
        period_now_subject_format = str(
            now.year) + str(now.month) + str(now.day)
        email_subject = str("[成效報表]" + spreadsheet_name + "_" +
                            period_now_subject_format + "_" + "更新" + condition)

        # 預定要寄給的負責人
        trafficker_name, email = [], []
        for j in trafficker_email:
            trafficker_name = j.get("name")
            email = j.get("email")
            email_text_body = "Dear" + trafficker_name + "：<br><br>    原預定產出" + spreadsheet_name + "：<br><br>    產出狀態：" + \
                condition + "<br>    客戶ID：" + customer_id + "<br>    報表產生時間：" + \
                period_now + "<br><br>Best Regards,<br>CW Robot"
            self.sendgridMail.send(email, email_subject, email_text_body)
