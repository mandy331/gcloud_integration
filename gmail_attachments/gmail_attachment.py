from __future__ import print_function

import base64
import os.path
import pickle
import urllib.error
import urllib.request as urllib2
from os import environ as env

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

load_dotenv()

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly','https://mail.google.com/','https://www.googleapis.com/auth/gmail.modify']

class GmailAttachment():

    def __init__(self):
        self.token_pickle = env.get("GMAIL_TOKEN_PICKLE", "gmail_token.pickle")
        self.gmail_credentials = env.get("GMAIL_CREDENTIALS", "gmail_credentials.json")
        self.service = None
        pass

    def cert(self):
        """Shows basic usage of the Gmail API.
        Lists the user's Gmail labels.
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(self.token_pickle):
            with open(self.token_pickle , 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.gmail_credentials, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_pickle , 'wb') as token:
                pickle.dump(creds, token)

        self.service = build('gmail', 'v1', credentials=creds)

    def get_profile(self, user_id):
        return self.service.users().getProfile(userId=user_id).execute()

    def list_messages(self, user_id):
        return self.service.users().messages().list(userId=user_id).execute()

    def get_message(self, user_id, message_id):
        try:
            message = self.service.users().messages().get(userId=user_id, id=message_id).execute()

            print ('Message snippet: %s' % message['snippet'])

            return message
        except urllib2.HTTPError as e:
            print ('An error occurred: %s' % e)



    def get_attachment(self, user_id, attachments_id, message_id, filename, store_dir):
        """Get and store attachment from Message with given id.

        Args:
            service: Authorized Gmail API service instance.
            user_id: User's email address. The special value "me"
            can be used to indicate the authenticated user.
            msg_id: ID of Message containing attachment.
            store_dir: The directory used to store attachments.
        """
        try:
            attach_result = self.service.users().messages().attachments().get(userId=user_id, 
                id=attachments_id, messageId=message_id).execute()
            path = ''.join([store_dir, filename])
            file_data = base64.urlsafe_b64decode(attach_result['data']
                                                        .encode('UTF-8'))

            f = open(path, 'wb')
            f.write(file_data)
            f.close()                    

        except urllib2.HTTPError as e:
            print ('An error occurred: %s' % error)

    def run(self, *args, **kwargs):

        self.cert()
        results = self.list_messages("me")
        if not results:
            print("No mails found.")
        else:
            print("Emails:")
            for email in results['messages'][0:100]:
                email_message = self.get_message('me', email['id'])
                print(email['id'])
                message_id = email['id']
                for item in email_message['payload']['headers']:
                    # print(item)
                    if item['name'] == 'Subject':
                        subject = item.get('value')
                    elif item['name'] == 'From':
                        From = item.get('value')

                # attachments_id = []
                print(subject, From)
                if 'parts' in email_message['payload']:
                    for item in email_message['payload']['parts']:
                        if item['filename']:
                            print(item['filename'])
                            print(item['body']['attachmentId'])
                            attachments_id = item['body']['attachmentId']
                            self.get_attachment('me', attachments_id, message_id, item['filename'], './attachments/')
