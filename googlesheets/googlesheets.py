from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from os import environ as env

from pprint import pprint
from googleapiclient import discovery
import pandas
import datetime

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly','https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
# SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
# SAMPLE_RANGE_NAME = 'Class Data!A2:E'

class GoogleSheets:

    def __init__(self, report):
            self.token_pickle = env.get("GOOGLESHEETS_TOKEN_PICKLE", "googlesheets_token.pickle")
            self.googlesheets_credentials = env.get("GOOGLESHEETS_CREDENTIALS", "googlesheets_credentials.json")
            self.service = None
            self.report = report
            pass

    def cert(self):

        """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(self.token_pickle):
            with open(self.token_pickle, 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.googlesheets_credentials, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_pickle, 'wb') as token:
                pickle.dump(creds, token)

        self.service = discovery.build('sheets', 'v4', credentials=creds)

    def run(self, *args, **kwargs):

        self.cert()
        
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
        
        read_template = self.get_template_values(params["template_spreadsheet_id"])
        read_palcementmap = self.get_placementmap_values(params['placementmap_spreadsheet_id'])
        merge_template_placementmap = self.merge_template_placementmap(read_template, read_palcementmap)
        create_spreadsheet_id = self.create_spreadsheet(params['spreadsheet_name'])

        campaign, campaign_numbers = self.count_campaign(self.report)

        for i in range(campaign_numbers):
            sheet_id = self.copy_template_to_sheets(params["template_spreadsheet_id"], params["template_sheet_id"], create_spreadsheet_id)
            self.rename_sheet(create_spreadsheet_id, sheet_id, campaign[i])   
            update_data = self.merge_report_data(merge_template_placementmap, self.report, campaign[i]) 
            self.update_values(create_spreadsheet_id, update_data)
        self.delete_first_sheets(create_spreadsheet_id)
        
    def get_template_values(self, template_spreadsheet_id):
        
        spreadsheet_id = template_spreadsheet_id  # TODO: Update placeholder value.

        # The A1 notation of the values to retrieve.
        range_ = '成效報表!4:4'  # TODO: Update placeholder value.

        majorDimension = 'ROW'

        # How values should be represented in the output.
        # The default render option is ValueRenderOption.FORMATTED_VALUE.
        value_render_option = 'FORMATTED_VALUE'  # TODO: Update placeholder value.

        # How dates, times, and durations should be represented in the output.
        # This is ignored if value_render_option is
        # FORMATTED_VALUE.
        # The default dateTime render option is [DateTimeRenderOption.SERIAL_NUMBER].
        date_time_render_option = 'SERIAL_NUMBER'  # TODO: Update placeholder value.

        request = self.service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_, valueRenderOption=value_render_option, dateTimeRenderOption=date_time_render_option)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        template_placement_col = response["values"][0]
        placement_col1, placement_col2, placement_col3, placement_name = [], [], [], []
        for i in range(1, len(template_placement_col)):
            if template_placement_col[i] != "":
                placement_name.append(template_placement_col[i])
                if i <= 26:
                    placement_col1.append(chr(64+i+1))
                    placement_col2.append(chr(64+i+2))
                    placement_col3.append(chr(64+i+3))
                else:
                    i = i - 26
                    placement_col1.append(chr(65)+chr(64+i+1))
                    placement_col2.append(chr(65)+chr(64+i+2))
                    placement_col3.append(chr(65)+chr(64+i+3))

        template = zip(placement_col1, placement_col2, placement_col3, placement_name)
        template_df = pandas.DataFrame(template,columns = ["Column1","Column2","Column3","版位名稱"]).reset_index(drop = True)
        print(template_df)

        return template_df   

    def get_placementmap_values(self, placement_map_id):

        spreadsheet_id = placement_map_id  # TODO: Update placeholder value.

        # The A1 notation of the values to retrieve.
        range_ = '工作表1'  # TODO: Update placeholder value.

        majorDimension = 'COLUMN'

        # How values should be represented in the output.
        # The default render option is ValueRenderOption.FORMATTED_VALUE.
        value_render_option = 'FORMATTED_VALUE'  # TODO: Update placeholder value.

        # How dates, times, and durations should be represented in the output.
        # This is ignored if value_render_option is
        # FORMATTED_VALUE.
        # The default dateTime render option is [DateTimeRenderOption.SERIAL_NUMBER].
        date_time_render_option = 'SERIAL_NUMBER'  # TODO: Update placeholder value.

        request = self.service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_, valueRenderOption=value_render_option, dateTimeRenderOption=date_time_render_option)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        placementmap_df = pandas.DataFrame(response["values"][1:],columns =["版位名稱","委刊項對照名稱"]).reset_index(drop = True) 

        return placementmap_df

    def merge_template_placementmap(self, template_df, placementmap_df):
        merge_template_placementmap = pandas.merge(template_df, placementmap_df, on = "版位名稱").reset_index(drop = True)
        print(merge_template_placementmap)
        return merge_template_placementmap

      
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

        pprint(response["spreadsheetId"])

    def copy_template_to_sheets(self, template_spreadsheet_id, template_sheet_id, create_spreadsheet_id):
        
        spreadsheet_id = template_spreadsheet_id # TODO: Update placeholder value.

        # The ID of the sheet to copy.
        sheet_id = template_sheet_id  # TODO: Update placeholder value.

        copy_sheet_to_another_spreadsheet_request_body = {
            # The ID of the spreadsheet to copy the sheet to.
            'destination_spreadsheet_id': create_spreadsheet_id,
        }

        request = self.service.spreadsheets().sheets().copyTo(spreadsheetId=spreadsheet_id, sheetId=sheet_id, body=copy_sheet_to_another_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response["sheetId"])
        
        sheet_id = response.get("sheetId", "")
        
        return sheet_id

    def delete_first_sheets(self, spreadsheet_id):
        
        spreadsheet_id = spreadsheet_id # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            # A list of updates to apply to the spreadsheet.
            # Requests will be applied in the order they are specified.
            # If any request is not valid, no requests will be applied.
            'requests': [
                {
                    "deleteSheet": {
                        "sheetId": 0 #預設刪掉第一個空的sheet，不知道這樣可不可以
                    }
                },
            ], 
        }

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)

    def rename_sheet(self, spreadsheet_id, sheet_id, campaign_name):

        spreadsheet_id = spreadsheet_id # TODO: Update placeholder value.

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

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)

    def merge_report_data(self, merge_template_placementmap, advertisement_report, compaign_name):
        
        # 篩選不同的報表
        advertisement_report = advertisement_report[advertisement_report["CAMPAIGN"] == compaign_name]

        # 算最早日期
        earliest_day = pandas.to_datetime(advertisement_report["DimensionAttribute.LINE_ITEM_START_DATE_TIME"].min())
        earliest_day = str(earliest_day.year)+ "/" + str(earliest_day.month) + "/" + str(earliest_day.day)
        earliest_day = datetime.datetime.strptime(earliest_day, '%Y/%m/%d')
        now = datetime.datetime.now()
        days = (now - earliest_day).days

        # 轉換日期格式
        def date_process(df):
            df = pandas.to_datetime(df)
            processed_date = str(df.month) + "/" + str(df.day)
            return processed_date

        advertisement_report['Dimension.DATE'] = advertisement_report['Dimension.DATE'].apply(date_process,1)
        advertisement_report['DimensionAttribute.LINE_ITEM_START_DATE_TIME'] = advertisement_report['DimensionAttribute.LINE_ITEM_START_DATE_TIME'].apply(date_process,1)

        # 合併 版位表、成效報表
        all_data = pandas.merge(advertisement_report, merge_template_placementmap, left_on = "PLACEMENT", right_on = "委刊項對照名稱")
        
        # 整理走期的數據
        period_df = pandas.DataFrame(all_data.groupby(['Column1','Column2','Column3'])['DimensionAttribute.LINE_ITEM_START_DATE_TIME'].min().reset_index(drop=False))

        # 將數據轉為可放入googlesheets的格式
        update_data = []

        # 將走期填入googlesheets
        today = str(now.month) + "/" + str(now.day)
        for x in range(len(period_df)):
            period = []
            period.append([str(period_df["DimensionAttribute.LINE_ITEM_START_DATE_TIME"][x])+ "-" + today]) 
            data = {
                    "range": compaign_name + "!" + str(period_df["Column1"][x])+ "6",
                    'majorDimension': 'ROWS',
                    "values":period,
                }
            update_data.append(data)

        # 日期
        Date = []
        day = earliest_day
        for i in range(days):
            Date.append(str(day.month) + "/" + str(day.day))
            day = day + datetime.timedelta(days=1)
        Date_df = pandas.DataFrame(columns = ["Date", "Index"])
        Date_df["Date"] = Date
        Date_df["Index"] = [i+8 for i in range(len(Date_df))] # 從第八行開始填入Imps.clicks.CTR
        print(Date_df)

        # 將日期填入googlesheets
        date = []
        for k in range(len(Date_df)):
            date.append([str(Date_df["Date"][k])]) 
        data = {
                "range": compaign_name + "!" + "A8:A",
                'majorDimension': 'ROWS',
                "values":date,
            }
        update_data.append(data)

        # 合併 日期、成效報表
        all_data = pandas.merge(all_data, Date_df, left_on = "Dimension.DATE", right_on = "Date")
        print(all_data)
        
        # 整理要放到googlesheets的成效數據，仍為DataFrame格式
        clean_data = pandas.DataFrame(all_data.groupby(['Column1','Column2','Column3','Index'])['Column.AD_SERVER_IMPRESSIONS','Column.AD_SERVER_CLICKS'].sum().reset_index(drop=False))
        clean_data["Column.AD_SERVER_CTR"] = round(clean_data["Column.AD_SERVER_CLICKS"] / clean_data["Column.AD_SERVER_IMPRESSIONS"], 4)
        clean_data['MIN'], clean_data['MAX'] = '', ''
        unique_column = list(clean_data['Column1'].unique())
        for i in unique_column:
            clean_data.loc[clean_data.Column1 == i, "MIN"] =  int(clean_data[clean_data['Column1'] == i].groupby(['Column1'])['Index'].min())
            clean_data.loc[clean_data.Column1 == i, "MAX"] =  int(clean_data[clean_data['Column1'] == i].groupby(['Column1'])['Index'].max())

        def Fill_Range(DataFrame):
            Range =  str(DataFrame["Column1"]) + str(DataFrame["MIN"]) + ":" + str(DataFrame["Column3"]) + str(DataFrame["MAX"])
            return Range

        clean_data['range'] = clean_data.apply(Fill_Range,1)
        print(clean_data)

        # 填入成效數據
        unique_range = list(set(clean_data["range"]))
        for i in unique_range:
            df = clean_data[clean_data["range"] == i].reset_index(drop = True)
            values = []
            for j in range(len(df)):
                values.append([int(df["Column.AD_SERVER_IMPRESSIONS"][j]),int(df["Column.AD_SERVER_CLICKS"][j]),int(df["Column.AD_SERVER_CTR"][j])])

            data = {
                    "range": compaign_name + "!" + i,
                    'majorDimension': 'ROWS',
                    "values":values,
                }
            update_data.append(data)
    
        return update_data
    
    def count_campaign(self, report):
        campaign = list(set(report["CAMPAIGN"]))
        campaign_numbers = len(campaign)
        return campaign, campaign_numbers
    
    def update_values(self, spreadsheet_id, update_data):