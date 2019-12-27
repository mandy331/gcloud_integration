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
SHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly','https://www.googleapis.com/auth/spreadsheets']
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file']

class GoogleSheets:

    def __init__(self, report, end_date):
            self.key = env.get("KEY","cw-web-prod-ad-manager.json")
            self.folder = env.get("SHARE_FOLDER")
            
            self.service = None
            self.sendgridMail = sendgridMail()
            
            self.report = report
            self.end_date = end_date

            self.template_spreadsheet_id = env.get("template_spreadsheet_id")
            self.template_sheet_id = env.get("template_sheet_id")
            self.start_row = 8
            self.default_template_total_row = 299
            self.column_df = self.default_template_sheet_column()
            pass
   
    def cert(self, SCOPES):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.key, SCOPES)
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
        
        if len(self.report) == 0:
            self.send_fail_mail(params["order_id"], params["trafficker_email"])
        
        else:
            campaign, campaign_count = self.count_campaign(self.report)
            create_spreadsheet_id = self.create_spreadsheet(self.report, self.end_date)

            for i in range(campaign_count):

                # 創建sheet
                sheet_id = self.copy_template_to_sheets(self.template_spreadsheet_id, self.template_sheet_id, create_spreadsheet_id)
                self.rename_sheet(create_spreadsheet_id, sheet_id, campaign[i])
                
                # 每個活動的報表
                campaign_report = self.get_campaign_report(self.report, campaign[i])
                
                # 整理每個活動的報表的日期
                early_day, days, months = self.campaign_month_count(campaign_report)

                # 填入 版位名稱、走期
                placement_index_df, last_column_index, update_data1 = self.fill_campaign_data(self.column_df, campaign_report, early_day, days)
                self.update_values(create_spreadsheet_id, update_data1)
                
                # 填入日期、數據、總和
                for k in range(months):
                    update_data2, total_start_index, last_row_index = self.fill_month_campaign_data(self.column_df, placement_index_df, campaign_report, early_day, k, days, self.start_row) 
                    self.copy_total_three_rows(create_spreadsheet_id, sheet_id, total_start_index)
                    self.update_values(create_spreadsheet_id, update_data2)    
                    self.default_template_total_row += 3          
                self.delete_empty_cols_rows(create_spreadsheet_id, sheet_id, last_column_index, last_row_index)    
                self.start_row = 8

            self.delete_first_sheet(create_spreadsheet_id)
            create_spreadsheet_url = self.get_url(create_spreadsheet_id)
            
            # 產出的報表放進google共用雲端資料夾
            self.cert(DRIVE_SCOPES)
            self.move_to_folder(self.folder, create_spreadsheet_id)
            self.send_successful_mail(params["order_id"], self.report, create_spreadsheet_url, params["trafficker_email"])
      
    def create_spreadsheet(self, report, end_date):
        
        format_end_date = end_date.strftime("%Y%m%d")

        spreadsheet_name = format_end_date + '_' + str(report["Dimension.ORDER_NAME"][0]) + '_成效' 

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
                        "sheetId": 0 #預設刪掉第一個空的sheet，不知道這樣可不可以
                    }
                },
            ], 
        }

        request = self.service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)
    
    def default_template_sheet_column(self):
        
        # 配合模板的版位欄位名稱      
        Column1 = ["B","E","H","K","N","Q","T","W","Z","AC","AF","AI","AL","AO","AR","AU","AX","BA","BD","BG","BJ","BM","BP","BS","BV","BY","CB","CE","CH","CK","CN","CQ","CT","CW","CZ","DC","DF","DI","DL","DO","DR","DU","DX"]
        Column2 = ["C","F","I","L","O","R","U","X","AA","AD","AG","AJ","AM","AP","AS","AV","AY","BB","BE","BH","BK","BN","BQ","BT","BW","BZ","CC","CF","CI","CL","CO","CR","CU","CX","DA","DD","DG","DJ","DM","DP","DS","DV","DY"]        
        Column3 = ["D","G","J","M","P","S","V","Y","AB","AE","AH","AK","AN","AQ","AT","AW","AZ","BC","AF","BI","BL","BO","BR","BU","BX","CA","CD","CG","CJ","CM","CP","CS","CV","CY","DB","DE","DH","DK","DN","DQ","DT","DW","DZ"]   
        column_df = zip(Column1, Column2, Column3)
        column_df = pandas.DataFrame(column_df, columns = ["Column1","Column2","Column3"])
        
        return column_df
    
    def count_campaign(self, report):
        
        campaign = list(set(report["Campaign"]))
        campaign_count = len(list(set(report["Campaign"])))
        
        return campaign, campaign_count
    
    def get_campaign_report(self, report, compaign_name):
        
        # 篩選不同的報表
        report = report[report["Campaign"] == compaign_name]

        return report
    
    def campaign_month_count(self, campaign_report):
        
        # 算委刊項中最早和最晚日期
        early_day = campaign_report["Dimension.DATE"].min()
        last_day = campaign_report["Dimension.DATE"].max()
        days = (last_day - early_day).days
        months = last_day.month - early_day.month + 1

        return early_day, days, months

    def fill_campaign_data(self, column_df, campaign_report, early_day, days):
        
        # 版位名稱和欄位位置匹配
        placement_list = sorted(list(set(campaign_report["版位名稱"])))
        placement_index_df = column_df[0:len(placement_list)]
        placement_index_df.loc[:, '版位名稱'] = placement_list
        last_column_index = 1 + len(placement_list)*3
        
        # 和版位名稱和版位index合併
        all_data = pandas.merge(campaign_report, placement_index_df, on = "版位名稱")

        # 整理各版位的走期
        unique_placement = list(set(all_data["Column1"]))
        period = []
        for i in range(len(unique_placement)):
            period_list = list(set(all_data[all_data["Column1"] == unique_placement[i]]["Dimension.DATE"]))
            start_day = min(period_list)
            end_day = max(period_list)
            days = (end_day - start_day).days
            if days == 0:
                period.append(str(start_day.month)+"/"+str(start_day.day))
            elif len(period_list) == days + 1:
                period.append(str(start_day.month)+"/"+str(start_day.day)+"-"+str(end_day.month)+"/"+str(end_day.day))
            else:
                many_period = ""
                first_day = start_day
                for i in range(days + 1):
                    current_day = start_day + datetime.timedelta(days=i)
                    current_after_day = current_day + datetime.timedelta(days=1)
                    current_before_day = current_day - datetime.timedelta(days=1)
                    if current_day in period_list:
                        if current_after_day not in period_list and current_before_day not in period_list:
                            many_period += str(current_day.month) + "/" + str(current_day.day) + "+"
                            first_day = current_after_day 
                        elif current_before_day not in period_list and current_after_day in period_list:
                            first_day = current_day
                            continue
                        elif current_before_day in period_list and current_after_day in period_list:
                            continue
                        elif current_before_day in period_list and current_after_day not in period_list:
                            last_day = current_day
                            many_period += str(first_day.month) + "/" + str(first_day.day) + "-" + str(last_day.month) + "/" + str(last_day.day) + "+"
                        else:
                            continue
                    else:
                        continue
                many_period = many_period.rstrip('+')
                period.append(many_period)

        period_df = zip(unique_placement,period)
        period_df = pandas.DataFrame(period_df, columns = ["Column1", "Period"])
        
        # 將數據轉為可放入googlesheets的格式
        update_data1 = []

        # 將走期填入googlesheets
        for x in range(len(period_df)):
            data = {
                    "range": str(all_data["Campaign"][0]) + "!" + str(period_df["Column1"][x])+ "6",
                    'majorDimension': 'ROWS',
                    "values":[[period_df["Period"][x]]],
                }
            update_data1.append(data)

        # 將版位名稱填入googlesheets
        for x in range(len(placement_index_df)):
            data = {
                    "range": str(all_data["Campaign"][0]) + "!" + str(placement_index_df["Column1"][x])+ "4",
                    'majorDimension': 'ROWS',
                    "values": [[placement_index_df["版位名稱"][x]]],
                }
            update_data1.append(data)
        
        return placement_index_df, last_column_index, update_data1   
    
    def fill_month_campaign_data(self, column_df, placement_index_df, campaign_report, early_day, k, days, start_row):
                
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
        Date_df = pandas.DataFrame(columns = ["Dimension.DATE", "Date_Format","Index"])
        Date_df["Dimension.DATE"], Date_df["Date_Format"] = Date, Date_Format
        Date_df["Index"] = [i + start_row for i in range(len(Date_df))] 
        
        # Total起始欄
        total_start_index = Date_df["Index"].max() + 1
        
        # 更新往下起始的Row數
        last_row_index = total_start_index + 3
        self.start_row = last_row_index

        # 合併 成效報表、版位名稱對照表、日期
        all_data = pandas.merge(campaign_report, Date_df, on = "Dimension.DATE")

        # 和版位名稱和版位index合併
        all_data = pandas.merge(all_data, placement_index_df, on = "版位名稱")

        # 將數據轉為可放入googlesheets的格式
        update_data2 = []
        
        # 整理填入數據
        clean_data = pandas.DataFrame(all_data.groupby(['Column1','Column2','Column3','Index'])['Column.AD_SERVER_IMPRESSIONS', 'Column.AD_SERVER_CLICKS'].sum().reset_index(drop=False))
        clean_data["Column.AD_SERVER_CTR"] = round(clean_data["Column.AD_SERVER_CLICKS"] / clean_data["Column.AD_SERVER_IMPRESSIONS"], 4)
        for j in range(len(clean_data)):
            values = []
            values.append([int(clean_data["Column.AD_SERVER_IMPRESSIONS"][j]),int(clean_data["Column.AD_SERVER_CLICKS"][j]),clean_data["Column.AD_SERVER_CTR"][j]])
            data = {
                    "range": str(all_data["Campaign"][0]) + "!" + str(clean_data["Column1"][j]) + str(clean_data["Index"][j]) +":"+ str(clean_data["Column3"][j]) + str(clean_data["Index"][j]),
                    "majorDimension": 'ROWS',
                    "values":values,
                }
            update_data2.append(data)
        
        # 整理Total三欄數據
        unique_column = list(set(clean_data["Column1"]))
        for j in range(len(unique_column)):
            df = clean_data[clean_data["Column1"] == unique_column[j]].reset_index(drop = True)
            data = {
                    "range": str(all_data["Campaign"][0]) + "!" + str(df["Column1"][0]) + str(total_start_index) +":"+ str(df["Column2"][0]) + str(total_start_index),
                    "majorDimension": 'ROWS',
                    "values":
                            [[int(sum(df["Column.AD_SERVER_IMPRESSIONS"])),int(sum(df["Column.AD_SERVER_CLICKS"]))]],
                    }

            update_data2.append(data)

        # 將日期填入googlesheets
        date = []
        for k in range(len(Date_df)):
            date.append([str(Date_df["Date_Format"][k])]) 
        data = {
                "range": str(all_data["Campaign"][0]) + "!A" + str(Date_df["Index"].min())  + ":A",
                'majorDimension': 'ROWS',
                "values":date,
            }
        update_data2.append(data)

        
        return update_data2, total_start_index, last_row_index
  
    def update_values(self, spreadsheet_id, update_data):

        spreadsheet_id = spreadsheet_id # TODO: Update placeholder value.

        batch_update_values_request_body = {
            # How the input data should be interpreted.
            'value_input_option': 'RAW',  # TODO: Update placeholder value.
            # The new values to apply to the spreadsheet.
            # append進去
            'data': update_data,

        }

        request = self.service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_values_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)
        
    def copy_total_three_rows(self, spreadsheet_id, sheet_id, total_start_index):

        spreadsheet_id = spreadsheet_id  # TODO: Update placeholder value.

        batch_update_spreadsheet_request_body = {
            "requests": [
                {    
                    "cutPaste":{
                        "source": {
                                "sheetId": sheet_id,
                                "startRowIndex": self.default_template_total_row,
                                "endRowIndex": self.default_template_total_row + 3,
                                "startColumnIndex": 0,
                                "endColumnIndex": 104,
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
                        "startIndex": int(last_row_index) - 1,
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
    
    

    def send_successful_mail(self, order_id, report, spreadsheet_url, trafficker_email):
        
        # 產出狀態
        condition = "成功"
        
        # 客戶名稱
        customer_name = str(report["Dimension.ORDER_NAME"][0])
        
        # 客戶ID
        customer_id = str(order_id)

        # 報表產生時間
        now = datetime.datetime.now()
        period_now = now.strftime('%Y/%m/%d %H:%M') 

        # 報表抓取數據時間區間 
        report["Dimension.DATE"] = pandas.to_datetime(report["Dimension.DATE"])
        early_day = report["Dimension.DATE"].min()
        last_day = report["Dimension.DATE"].max()
        period_start = early_day.strftime('%Y/%m/%d %H:%M') 
        period_end = last_day.strftime('%Y/%m/%d %H:%M')
        period_time = period_start + " - " + period_end
        
        # 報表連結
        spreadsheet_url = str(spreadsheet_url)

        # email格式
        period_now_subject_format = str(now.year) + str(now.month) + str(now.day)
        email_subject = str("[成效報表]" + customer_name + "_" + period_now_subject_format + "_" + "更新" + condition)
        
        # 預定要寄給的負責人
        trafficker_name, email = [], []
        for j in trafficker_email:
            trafficker_name.append(j.get("name"))
            email.append(j.get("email"))

        # 負責人姓名和負責人信箱
        trafficker = report["DimensionAttribute.ORDER_TRAFFICKER"]
        pattern = r"(.*)(\s)[(](.*)[)]"
        for person in trafficker:
            result = re.findall(pattern, person)
            if result[0][0] not in trafficker_name:
                trafficker_name.append(result[0][0])
                email.append(result[0][2])
        
        tra = zip(trafficker_name, email)
        tra_df = pandas.DataFrame(tra, columns = ["負責人","Email"]).drop_duplicates().reset_index(drop=True)

        for i in range(len(tra_df)):
            trafficker_name = str("閔慈")
            trafficker_email = str("mhuang98331@gmail.com")
            #trafficker_name = str(tra_df["負責人"][i])
            #trafficker_email = str(tra_df["Email"][i])
            email_text_body = "Dear" + trafficker_name + "：<br><br>" + "    以下為" + customer_name + "的成效報表資訊：" + "<br><br>    產出狀態：" + condition + "<br>    客戶ID：" + customer_id + "<br>    報表產生時間：" + period_now + "<br>    報表抓取數據時間區間：" + period_time + "<br>    報表連結：" + spreadsheet_url + "<br><br>Best Regards,<br>CW Robot"
            self.sendgridMail.send(trafficker_email, email_subject, email_text_body) 
    
    def send_fail_mail(self, order_id, trafficker_email):
        
        # 產出狀態
        condition = "失敗"
                
        # 客戶ID
        customer_id = str(order_id)

        # 報表產生時間
        now = datetime.datetime.now()
        period_now = now.strftime('%Y/%m/%d %H:%M') 

        # email格式
        period_now_subject_format = str(now.year) + str(now.month) + str(now.day)
        email_subject = str("[成效報表]" + customer_id + "_" + period_now_subject_format + "_" + "更新" + condition)
        
        # 預定要寄給的負責人
        trafficker_name, email = [], []
        for j in trafficker_email:
            trafficker_name = j.get("name")
            email = j.get("email")
            email_text_body = "Dear" + trafficker_name + "：" + "<br><br>    產出狀態：" + condition + "<br>    客戶ID：" + customer_id + "<br>    報表產生時間：" + period_now + "<br><br>Best Regards,<br>CW Robot"
            self.sendgridMail.send(email, email_subject, email_text_body)
        
        

        

    