# from __future__ import print_function
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
from dateutil.relativedelta import relativedelta
import re

# If modifying these scopes, delete the file token.pickle.
SHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly','https://www.googleapis.com/auth/spreadsheets']
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file']

class GoogleSheets:

    def __init__(self, report, start_date, end_date):
            self.DFP_KEY = env.get("DFP_KEY","cw-web-prod-ad-manager.json")
            self.folder = env.get("SHARE_FOLDER")
            
            self.service = None
            
            self.report = report
            self.end_date = end_date
            self.start_date = start_date

            self.template_spreadsheet_id = env.get("TEMPLATE_SPREADSHEET_ID")
            self.template_sheet_id = env.get("TEMPLATE_SHEET_ID")
            self.start_row = 8
            self.default_template_three_total_row = 299
            pass
   
    def cert(self, SCOPES):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.DFP_KEY, SCOPES)
        if SCOPES == SHEETS_SCOPES:
            self.service = build('sheets', 'v4', credentials=credentials)
        elif SCOPES == DRIVE_SCOPES:
            delegated_credentials = credentials.create_delegated('robot@cw.com.tw')
            self.service = build('drive', 'v3', credentials=credentials)

    def run(self, *args, **kwargs):

        self.cert(SHEETS_SCOPES)
        
        if args[0] is None:
            return None

        params = args[0]

        if "prebuy" in params:
            prebuy_data = self.clean_prebuy_data(params['prebuy'], self.start_date, self.end_date)
        
        campaign, campaign_count = self.get_campaign_list(self.report)
        create_spreadsheet_id = self.create_spreadsheet(self.report, self.start_date, self.end_date)
        column_df = self.default_template_sheet_column()

        for i in range(campaign_count):
            # 創建sheet
            sheet_id = self.copy_template_to_sheets(self.template_spreadsheet_id, self.template_sheet_id, create_spreadsheet_id)
            self.rename_sheet(create_spreadsheet_id, sheet_id, campaign[i])
            
            # 每個活動的報表
            campaign_report = self.get_campaign_report(self.report, campaign[i])

            # 填入 Advertiser、Period、版位名稱、走期、日期、數據
            column_index_df, unique_placement, last_column_index, last_row_index, total_index_df, last_total_index, prebuy_index_df, last_prebuy_index, first_day_df, all_data, update_data1, no_data_index_list = self.fill_campaign_data(campaign[i], column_df, campaign_report, self.start_date, self.end_date)
            self.update_values(create_spreadsheet_id, update_data1)
            
            # 複製 Total、Prebuy、達成率 三列
            for k in range(len(total_index_df)):
                self.copy_total_three_rows(create_spreadsheet_id, sheet_id, total_index_df["Index"][k])
                self.default_template_three_total_row += 3
            self.copy_total_three_rows(create_spreadsheet_id, sheet_id, last_total_index)
            self.default_template_three_total_row += 3

            # 填入Total 數據
            update_data2 = self.fill_total_data(campaign[i], unique_placement, total_index_df, last_total_index, first_day_df, all_data)
            self.update_values(create_spreadsheet_id, update_data2)

            # 填入Prebuy 數據
            if 'prebuy_data' in locals().keys():
                campaign_prebuy_data = self.get_prebuy_data(prebuy_data, campaign[i])
                new_campaign_prebuy_data, new_column_index_df = self.clean_column_name(campaign_prebuy_data, column_index_df)
                update_data3 = self.fill_prebuy_data(campaign[i], new_campaign_prebuy_data, new_column_index_df, prebuy_index_df, last_prebuy_index)
                self.update_values(create_spreadsheet_id, update_data3)

            # 將空的row填入顏色
            color_requests = self.get_background_color(sheet_id, no_data_index_list)
            self.change_background_color(create_spreadsheet_id, color_requests)
            
            # 刪除空的行列
            self.delete_empty_cols_rows(create_spreadsheet_id, sheet_id, last_column_index, last_row_index)

        self.delete_first_sheet(create_spreadsheet_id)
        spreadsheet_url = self.get_url(create_spreadsheet_id)
        
        # 產出的報表放進google共用雲端資料夾
        self.cert(DRIVE_SCOPES)
        self.move_to_folder(self.folder, create_spreadsheet_id)     
        
        if "traffickers" in params:
            new_trafficker_email = self.clean_trafficker_email(self.report, params["traffickers"])
        else:
            new_trafficker_email = self.clean_trafficker_email(self.report)

        return spreadsheet_url, new_trafficker_email
    
    def clean_prebuy_data(self, prebuy, start_date, end_date):
        
        # 走期橫跨的月份清單
        months = end_date.month - start_date.month
        days = (end_date - start_date).days

        def check_year(start_date, end_date):
            year = end_date.year - start_date.year
            if year > 0:
                return True
            else:
                return False
        
        if check_year(start_date, end_date):
            months = months + 12

        month_list = [start_date.strftime("%Y%m")]
        for i in range(days+1):
            new_day = start_date + datetime.timedelta(days=i)
            new_day_format = new_day.strftime("%Y%m")
            if new_day_format not in month_list:
                month_list.append(new_day_format)
              
        def clean_prebuy_name(placement):
            placement = placement.lstrip()
            pattern = r"\[(.*)\](.*)"
            if placement[0] == "[":
                result = re.findall(pattern, placement)
                prebuy_campaign = result[0][0]
                prebuy_placement = result[0][1].strip()
            else:
                prebuy_campaign = "成效報表"
                prebuy_placement = placement.strip()
            return prebuy_campaign, prebuy_placement
                
        # 從參數中獲取prebuy數據
        placement_list, prebuy_campaign_list, year_month_list, imps, clicks = [], [], [], [], []
        for prebuy_month in prebuy[0]:
            if prebuy_month in month_list or prebuy_month == "total":
                for placement in prebuy[0][prebuy_month].keys():
                    
                    prebuy_campaign, prebuy_placement = clean_prebuy_name(placement)
                    prebuy_campaign_list.append(prebuy_campaign)
                    placement_list.append(prebuy_placement)
                    
                    year_month_list.append(prebuy_month)
                    
                    if "impressions" in prebuy[0][prebuy_month][placement]:
                        imps.append(prebuy[0][prebuy_month][placement]["impressions"])
                    else:
                        imps.append(-1)
                    if "clicks" in prebuy[0][prebuy_month][placement]:
                        clicks.append(prebuy[0][prebuy_month][placement]["clicks"])
                    else:
                        clicks.append(-1)
        
        prebuy_data_df = zip(placement_list, prebuy_campaign_list, imps, clicks, year_month_list)
        prebuy_data = pandas.DataFrame(prebuy_data_df, columns = ["版位名稱", "Campaign", "imps", "clicks", "year_month"])

        return prebuy_data


    def create_spreadsheet(self, report, start_date, end_date):
        
        format_end_date = end_date.strftime("%Y%m%d")
        format_start_date = start_date.strftime("%Y%m%d")
        now = datetime.date.today().strftime("%Y%m%d")

        spreadsheet_name = "{}_{}_{}-{}_成效"
        spreadsheet_name = spreadsheet_name.format(now, report["Dimension.ORDER_NAME"][0], format_start_date, format_end_date)

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

    def delete_first_sheet(self, spreadsheet_id):
        
        spreadsheet_id = spreadsheet_id # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            # A list of updates to apply to the spreadsheet.
            # Requests will be applied in the order they are specified.
            # If any request is not valid, no requests will be applied.
            'requests': [
                {
                    "deleteSheet": {
                        "sheetId": 0 # 預設刪掉第一個空的sheet
                    }
                },
            ], 
        }

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)
    
    def default_template_sheet_column(self):
        
        # 配合模板的Column名稱（只能是英文），最多可以有234個版位
        alphabet = [chr(i) for i in range(66,91)]
        for k in range(26):
            k = k + 65
            alphabet = alphabet + [chr(k)+chr(i) for i in range(65,91)]
        alphabet.append("AAA")

        # 每三欄一個版位
        Column1 = [alphabet[x] for x in range(len(alphabet)) if x%3 == 0]
        Column2 = [alphabet[x] for x in range(len(alphabet)) if x%3 == 1]
        Column3 = [alphabet[x] for x in range(len(alphabet)) if x%3 == 2]

        column_df = zip(Column1, Column2, Column3)
        column_df = pandas.DataFrame(column_df, columns = ["Column1", "Column2", "Column3"])
        
        return column_df    
    
    def get_campaign_list(self, report):
        
        campaign = list(set(report["Campaign"]))
        campaign_count = len(campaign)
        
        return campaign, campaign_count
    
    def get_campaign_report(self, report, compaign_name):
        
        # 篩選不同的報表
        report = report[report["Campaign"] == compaign_name]
        report = report.reset_index(drop = True)
        return report
    
    def update_data_format(self, sheet_range, majorDimension, values):

        data = {
                    "range": sheet_range,  # sheet_range : str, eg. {sheet_name}!{column}{row}:{column}(row) 
                    'majorDimension': majorDimension, # majorDimension : str, "COLUMNS" or "ROWS"
                    "values": [values], # values : list
                }
        
        return data
    
    def fill_campaign_data(self, compaign_name, column_df, campaign_report, start_date, end_date):
                    
        ## 將數據轉為可放入googlesheets的格式
        update_data1 = []

        ## 整理Advertiser、Period數據
        agency = "Agency："
        advertiser = "Advertiser：{}".format(campaign_report["Dimension.ORDER_NAME"][0])
        format2_start_date = start_date.strftime("%m/%d")
        format2_end_date = end_date.strftime("%m/%d")
        all_period = "Period：{}-{}".format(format2_start_date, format2_end_date)
        
        # Advertiser、Period填入googlesheets
        update_data1.append(self.update_data_format("{}!A1:A3".format(compaign_name), 'COLUMNS', [agency, advertiser, all_period]))
           
        
        ## 版位名稱
        # 版位名稱和欄位位置匹配
        placement_list = sorted(list(set(campaign_report["版位名稱"])))
        column_index_df = column_df[0:len(placement_list)] 
        column_index_df.loc[:, '版位名稱'] = placement_list 
        
        # 版位名稱填入googlesheets
        for x in range(len(column_index_df)):
            placement_range = "{}!{}4".format(compaign_name, column_index_df["Column1"][x])
            values = [column_index_df["版位名稱"][x]]
            update_data1.append(self.update_data_format(placement_range, "ROWS", values))
        
        
        ## 定義最後一欄的位置
        last_column_index = len(placement_list)*3 + 1 # 開始欄是B欄
        
        
        ## 版位數據和版位Column index合併
        campaign_index_df = pandas.merge(campaign_report, column_index_df, on = "版位名稱")

        
        ## 走期
        unique_placement = list(set(campaign_index_df["Column1"]))
        period = []
        for i in range(len(unique_placement)):
            period_list = list(set(campaign_index_df["Dimension.DATE"][campaign_index_df["Column1"] == unique_placement[i]]))
            start_day = min(period_list)
            end_day = max(period_list)
            days = (end_day - start_day).days
            if days == 0:
                period_str1 = "{}/{}".format(start_day.month, start_day.day)
                period.append(period_str1)
            elif len(period_list) == days + 1:
                period_str2 = "{}/{}-{}/{}".format(start_day.month, start_day.day, end_day.month, end_day.day)
                period.append(period_str2)
            else:
                many_period = ""
                first_day = start_day
                for i in range(days + 1):
                    current_day = start_day + datetime.timedelta(days=i)
                    current_after_day = current_day + datetime.timedelta(days=1)
                    current_before_day = current_day - datetime.timedelta(days=1)
                    if current_day in period_list:
                        if current_after_day not in period_list and current_before_day not in period_list:
                            many_period += "{}/{}+".format(current_day.month, current_day.day)
                            first_day = current_after_day 
                        elif current_before_day not in period_list and current_after_day in period_list:
                            first_day = current_day
                            continue
                        elif current_before_day in period_list and current_after_day in period_list:
                            continue
                        elif current_before_day in period_list and current_after_day not in period_list:
                            last_day = current_day
                            many_period += "{}/{}-{}/{}+".format(first_day.month, first_day.day, last_day.month, last_day.day)
                        else:
                            continue
                    else:
                        continue
                many_period = many_period.rstrip('+')
                period.append(many_period)
        
        period_df = zip(unique_placement, period)
        period_df = pandas.DataFrame(period_df, columns = ["Column1", "Period"])

        # 走期填入googlesheets
        for x in range(len(period_df)):
            period_range = "{}!{}6".format(compaign_name, period_df["Column1"][x])
            values = [period_df["Period"][x]]
            update_data1.append(self.update_data_format(period_range, 'ROWS', values))
        
        
        ## 左欄Date
        def insert_month_last_3_index(Date_list, Display_list, day):
            Date_list = Date_list + [day] * 3 
            Display_list = Display_list + ["Total(Month)", "Pre-buy(Month)", "達成率(Month)"]
            return Date_list, Display_list
        
        # 整理Date數據
        days = (end_date - start_date).days
        Date, Display = [], []
        for i in range(days+1):
            new_day = start_date + datetime.timedelta(days=i)
            before_new_day = new_day - datetime.timedelta(days=1)
            if before_new_day > start_date and new_day.month != before_new_day.month:
                Date, Display = insert_month_last_3_index(Date, Display, before_new_day) # 加入每個月小結的列數，且Date為該月最後一天
            Date.append(new_day)
            Display.append("{}/{}".format(new_day.month, new_day.day))
        Date, Display = insert_month_last_3_index(Date, Display, end_date) # 最後一個月的小結

        row_index_df = pandas.DataFrame(columns = ["Dimension.DATE", "Display","Index"])
        row_index_df["Dimension.DATE"], row_index_df["Display"] = Date, Display
        row_index_df["year_month"] = row_index_df["Dimension.DATE"].apply(lambda x:x.strftime("%Y%m"))
        row_index_df["Index"] = [i + self.start_row for i in range(len(row_index_df))]
        
        ### row_index_df [start_date:2019-12-30, end_date:2020-01-02]
        # Dimension.DATE     Display          Index    year_month
        # 2019-12-30          12/30             8      201912
        # 2019-12-31          12/31             9      201912
        # 2019-12-31       Total(Month)        10      201912
        # 2019-12-31       Pre-buy(Month)      11      201912
        # 2019-12-31          達成率            12      201912
        # 2020-01-01           1/1             13      202001
        # 2020-01-02           1/2             14      202001
        # 2020-01-02       Total(Month)        15      202001
        # 2020-01-02       Pre-buy(Month)      16      202001 
        # 2020-01-02          達成率            17      202001

        # 左欄Date填入googlesheets
        date = []
        for k in range(len(row_index_df)):
            date.append(str(row_index_df["Display"][k])) 
        update_data1.append(self.update_data_format("{}!A{}:A".format(compaign_name, row_index_df["Index"].min()), 'COLUMNS', date))
        
        
        ## 定義Total. Pre-buy列數
        last_total_index = row_index_df["Index"].max() + 1
        last_prebuy_index = row_index_df["Index"].max() + 2
        
        ## 定義最後一列的位置
        last_row_index = last_total_index + 2

        ## 紀錄日期和Total、Pre-buy列的Index
        data_index_df = row_index_df[~(row_index_df["Display"].str.contains("Total")) & ~(row_index_df["Display"].str.contains("Pre-buy")) & ~(row_index_df["Display"].str.contains("達成率"))].reset_index(drop =True)
        total_index_df = row_index_df[row_index_df["Display"] == "Total(Month)"].reset_index(drop = True)
        prebuy_index_df = row_index_df[row_index_df["Display"] == "Pre-buy(Month)"].reset_index(drop =True)

        ## 紀錄各月分的第一天的Index
        unique_month = list(set(data_index_df["year_month"]))
        each_month_first_day = []
        for i in unique_month:
            first_day = data_index_df["Dimension.DATE"][data_index_df["year_month"] == i].min()
            each_month_first_day.append(int(data_index_df["Index"][data_index_df["Dimension.DATE"] == first_day]))
        
        first_day_df = zip(unique_month, each_month_first_day)
        first_day_df = pandas.DataFrame(first_day_df, columns = ["year_month","first_day_Index"])
            
        ## 合併 成效報表、版位名稱對照表、日期
        all_data = pandas.merge(campaign_index_df, data_index_df, on = "Dimension.DATE")

        
        ## 每日數據
        clean_data = pandas.DataFrame(all_data.groupby(['Column1','Column2','Column3','Index'])['Column.AD_SERVER_IMPRESSIONS', 'Column.AD_SERVER_CLICKS'].sum().reset_index(drop=False))
        clean_data["Column.AD_SERVER_CTR"] = round(clean_data["Column.AD_SERVER_CLICKS"] / clean_data["Column.AD_SERVER_IMPRESSIONS"] * 100, 2)
                
        # 填入每日數據
        for j in range(len(clean_data)):
            data_range = "{}!{}{}:{}{}".format(compaign_name, clean_data["Column1"][j], clean_data["Index"][j], clean_data["Column3"][j], clean_data["Index"][j])
            values = []
            values.append(int(clean_data["Column.AD_SERVER_IMPRESSIONS"][j]))
            values.append(int(clean_data["Column.AD_SERVER_CLICKS"][j]))
            values.append('{:.2f}%'.format(clean_data["Column.AD_SERVER_CTR"][j]))
            update_data1.append(self.update_data_format(data_range, 'ROWS', values))

        # 找出沒有數據的天數
        data_index_list = set(data_index_df["Index"])
        have_data_index_list = set(clean_data["Index"])
        no_data_index_list = list(data_index_list.difference(have_data_index_list))
    
        return column_index_df, unique_placement, last_column_index, last_row_index, total_index_df, last_total_index, prebuy_index_df, last_prebuy_index, first_day_df, all_data, update_data1, no_data_index_list 
          
    def fill_total_data(self, compaign_name, unique_placement, total_index_df, last_total_index, first_day_df, all_data):
        
        ## 將數據轉為可放入googlesheets的格式
        update_data2 = []

        ## 塡入月結文字
        for k in range(len(total_index_df)):
            total_range = "{}!A{}".format(compaign_name, total_index_df["Index"][k])
            values = []
            values.append(total_index_df["Display"][k])
            update_data2.append(self.update_data_format(total_range, 'ROWS', values))
        
        ## 填入大結文字
        total_range2 = "{}!A{}".format(compaign_name, last_total_index)
        values2 = ["Total"]
        update_data2.append(self.update_data_format(total_range2, 'COLUMNS', values2))
        

        def get_total_formula(df, total_month_index_list):
            total_imps = "="
            total_clicks = "="
            for p in total_month_index_list:
                total_imps += "{}{}+".format(df["Column1"][0], p)
                total_clicks += "{}{}+".format(df["Column2"][0], p)
            total_imps = total_imps.rstrip('+')
            total_clicks = total_clicks.rstrip('+')
            return total_imps, total_clicks     

        ## 填入Total數據
        for j in unique_placement:
            df = all_data[all_data["Column1"] == j].reset_index(drop=True)
            for k in range(len(total_index_df)):
                first_day_index = int(first_day_df["first_day_Index"][first_day_df["year_month"] == total_index_df["year_month"][k]])
                last_day_index = int(total_index_df["Index"][k]) - 1
                values3 = ["=SUM({}{}:{}{})".format(df["Column1"][0], first_day_index, df["Column1"][0], last_day_index), "=SUM({}{}:{}{})".format(df["Column2"][0], first_day_index, df["Column2"][0], last_day_index)]
                
                total_range3 = "{}!{}{}:{}{}".format(compaign_name, df["Column1"][0], total_index_df["Index"][k], df["Column2"][0], total_index_df["Index"][k])
                update_data2.append(self.update_data_format(total_range3, 'ROWS', values3)) 

            # 填入大結Total數據
            total_month_index_list = list(total_index_df["Index"])
            total_imps, total_clicks = get_total_formula(df, total_month_index_list)
            values4 = [total_imps, total_clicks]
            total_range4 = "{}!{}{}:{}{}".format(compaign_name, df["Column1"][0], last_total_index, df["Column2"][0], last_total_index)
            update_data2.append(self.update_data_format(total_range4, 'ROWS', values4)) 

        return update_data2

    def get_prebuy_data(self, prebuy_data, campaign_name):

        campaign_prebuy_data = prebuy_data[prebuy_data["Campaign"] == campaign_name].reset_index(drop = True)

        return campaign_prebuy_data
    
    def clean_column_name(self, campaign_prebuy_data, column_index_df):
        
        campaign_prebuy_data["版位名稱2"] = campaign_prebuy_data["版位名稱"].apply(lambda x:str(x).replace(" ","").replace("_","").replace("__","").replace("＿",""))

        def clean_campaign(placement):
            placement = placement.lstrip()
            pattern2 = r"((.*)_(.*))"
            result2 = re.findall(pattern2, placement)
            placement2 = result2[0][1]
            return placement2      
        
        column_index_df["版位名稱2"] = column_index_df["版位名稱"].apply(lambda x:clean_campaign(x).replace(" ","").replace("_","").replace("__","").replace("＿",""))
      
        return campaign_prebuy_data, column_index_df
        
    def fill_prebuy_data(self, compaign_name, new_campaign_prebuy_data, new_column_index_df, prebuy_index_df, last_prebuy_index):
        
        ## 將數據轉為可放入googlesheets的格式
        update_data3 = []
        
        ## 塡入月結文字
        for k in range(len(prebuy_index_df)):
            prebuy_range = "{}!A{}".format(compaign_name, prebuy_index_df["Index"][k])
            values = []
            values.append(prebuy_index_df["Display"][k])
            update_data3.append(self.update_data_format(prebuy_range, 'ROWS', values))
        
        ## 填入大結文字
        prebuy_range2 = "{}!A{}".format(compaign_name, last_prebuy_index)
        values2 = ["Pre-buy"]
        update_data3.append(self.update_data_format(prebuy_range2, 'COLUMNS', values2))
        
        def get_prebuy_month_data(new_campaign_prebuy_data, new_column_index_df, prebuy_index_df):
            prebuy_data1 = pandas.merge(new_campaign_prebuy_data, new_column_index_df, on = "版位名稱2")
            prebuy_data2 = pandas.merge(prebuy_data1, prebuy_index_df, on = "year_month")
            prebuy_data3 = prebuy_data2.dropna(subset = ["Column1", "Index"], inplace = False).reset_index(drop = True)
            return prebuy_data3
        
        def get_prebuy_total_data(new_campaign_prebuy_data, new_column_index_df):
            prebuy_total_df = new_campaign_prebuy_data[(new_campaign_prebuy_data["year_month"] == "total")]
            new_column_index_df = new_column_index_df[["Column1", "Column2", "版位名稱2"]]
            prebuy_total_data = pandas.merge(prebuy_total_df, new_column_index_df, on = "版位名稱2", how = "left")
            prebuy_total_data = prebuy_total_data.dropna(subset = ["Column1", "版位名稱2"], inplace = False).reset_index(drop = True)
            return prebuy_total_data
        
        def fill_data(prebuy_df, index = None):
            for i in range(len(prebuy_df)):
                if index == None:
                    index2 = prebuy_df["Index"][i]
                else:
                    index2 = index

                if prebuy_df["imps"][i] != -1:
                    prebuy_range3 = "{}!{}{}".format(compaign_name, prebuy_df["Column1"][i], index2)
                    values3 = [int(prebuy_df["imps"][i])]
                    update_data3.append(self.update_data_format(prebuy_range3, 'ROWS', values3))
                
                if prebuy_df["clicks"][i] != -1:
                    prebuy_range4 = "{}!{}{}".format(compaign_name, prebuy_df["Column2"][i], index2)
                    values4 = [int(prebuy_df["clicks"][i])]
                    update_data3.append(self.update_data_format(prebuy_range4, 'ROWS', values4))
          
            return update_data3

        
        ## 填入Pre-buy數據
        if not get_prebuy_month_data(new_campaign_prebuy_data, new_column_index_df, prebuy_index_df).empty:
            # Pre-buy(Month)
            prebuy_month_df = get_prebuy_month_data(new_campaign_prebuy_data, new_column_index_df, prebuy_index_df)
            update_data3 = fill_data(prebuy_month_df)

            # Pre-buy 
            prebuy_total_df = get_prebuy_total_data(new_campaign_prebuy_data, new_column_index_df)
            update_data3 = fill_data(prebuy_total_df, last_prebuy_index)

        return update_data3
        
    def update_values(self, spreadsheet_id, update_data):

        spreadsheet_id = spreadsheet_id # TODO: Update placeholder value.

        batch_update_values_request_body = {
            # How the input data should be interpreted.
            'value_input_option': 'USER_ENTERED',  # TODO: Update placeholder value.
            # The new values to apply to the spreadsheet.
            'data': update_data,

        }

        request = self.service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_values_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)
    
    def get_background_color(self, sheet_id, no_data_index_list):

        color_requests = []
        
        for k in no_data_index_list:
            requests = {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": k - 1,
                        "endRowIndex": k,
                        "startColumnIndex": 0,
                        "endColumnIndex": 133,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 0.788,
                                "green": 0.854,
                                "blue": 0.972,
                                "alpha": 1.0
                            }
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor)"
                }
            }

            color_requests.append(requests)
        
        return color_requests


    def change_background_color(self, spreadsheet_id, color_requests):
        
        body = {
            'requests': color_requests
        }
        
        response = self.service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body).execute()
        
    def copy_total_three_rows(self, spreadsheet_id, sheet_id, total_start_index):

        spreadsheet_id = spreadsheet_id  # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            "requests": [
                {    
                    "cutPaste":{
                        "source": {
                                "sheetId": sheet_id,
                                "startRowIndex": self.default_template_three_total_row,
                                "endRowIndex": self.default_template_three_total_row + 3,
                                "startColumnIndex": 0,
                                "endColumnIndex": 133,
                            },
                        "destination": {
                                "sheetId": sheet_id,
                                "rowIndex": int(total_start_index) - 1,
                                "columnIndex": 0
                            },
                        "pasteType": "PASTE_NORMAL",
                    }
                }
            ]
        }

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)
  
            
    def delete_empty_cols_rows(self, spreadsheet_id, sheet_id, last_column_index, last_row_index):
        
        spreadsheet_id = spreadsheet_id # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            'requests': [
                {
                    "deleteDimension": {
                        "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": int(last_column_index),
                        "endIndex": 130
                        }
                    }
                },
                {
                    "deleteDimension": {
                        "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": int(last_row_index),
                        "endIndex": 797
                        }
                    }
                },

            ],

        }

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body)
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

        request = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id, ranges=ranges, includeGridData=include_grid_data)
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
    
    
    def clean_trafficker_email(self, report, traffickers = None):
        
        data = {}
        # 預定要寄給的負責人
        if traffickers:
            for j in traffickers:
                if j.get("email"):
                    data[j.get("email")] = j.get("name")

        # 負責人姓名和負責人信箱
        trafficker = report["DimensionAttribute.ORDER_TRAFFICKER"]
        pattern = r"(.*)(\s)[(](.*)[)]"
        for person in trafficker:
            result = re.findall(pattern, person)
            if result[0][2] not in data.keys():
                data[result[0][2]] = result[0][0]
        
        return data

        
        

        

    