from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pprint import pprint
from googleapiclient import discovery


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly','https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
# SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
# SAMPLE_RANGE_NAME = 'Class Data!A2:E'


class googlesheets:

    def __init__(self):
            self.token_pickle = env.get("GOOGLESHEETS_TOKEN_PICKLE", "googlesheets_token.pickle")
            self.googlesheets_credentials = env.get("GOOGLESHEETS_CREDENTIALS", "googlesheets_credentials.json")
            self.service = None
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

        self.service = build('sheets', 'v4', credentials=creds)

    def create_spreadsheet(self, spreadsheet_name):
        
        spreadsheet = {
            'properties': {
                'title': spreadsheet_name
            }
        }
        spreadsheet = service.spreadsheets().create(body=spreadsheet,
                                            fields='spreadsheetId')
        response = spreadsheet.execute()

        return response["spreadsheetId"]

    def get_template_values(self, template_spreadsheet_id, template_sheet_id):
        
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

        request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_, valueRenderOption=value_render_option, dateTimeRenderOption=date_time_render_option)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        template_placement_col = response["values"][0]
        placement_col, placement_name = [], []
        for i in range(1, len(template_placement_col)):
            if template_placement_col[i] != "":
                placement_name.append(template_placement_col[i])
                if i <= 26:
                    placement_col.append(chr(64+i))
                else:
                    i = i - 26
                    placement_col.append(chr(65)+chr(64+i))

        placement = zip(placement_col, placement_name)
        placement_df = pd.DataFrame(placement,columns = ["Column","版位名稱"])
        print(placement_df)

        return placement_df         

    def copy_template_to_sheets(self, template_spreadsheet_id, template_sheet_id, spreadsheet_id):
        
        spreadsheet_id =  template_spreadsheet_id # TODO: Update placeholder value.

        # The ID of the sheet to copy.
        sheet_id = template_sheet_id  # TODO: Update placeholder value.

        copy_sheet_to_another_spreadsheet_request_body = {
            # The ID of the spreadsheet to copy the sheet to.
            'destination_spreadsheet_id': spreadsheet_id,
        }

        request = service.spreadsheets().sheets().copyTo(spreadsheetId=spreadsheet_id, sheetId=sheet_id, body=copy_sheet_to_another_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response["sheetId"])
        
        return response["sheetId"]

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

        request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)

    def rename_sheet(self, spreadsheet_id, sheet_id, sheet_name):

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
                        "title": sheet_name,
                    },
                    "fields": "title"
                    }
                }
            ],  
        }

        request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)