## Gmail 服務整合紀錄

### 目錄結構：
- admanager: 廣告系統串接
- attachments: 下載EMAIL附件存放的目錄
- cert: 存放各種認證檔案
- gmail_attachment: GMAIL串接
- spreadsheet: Google Spreadsheet 串接

### .env 環境參數：

|Key|Value|Desc|
|---|-----|----|
|GOOGLEADS_YAML|cert/googleads.yaml|AD Manager 設定檔位置|
|GMAIL_CREDENTIALS|cert/robot-gmail-credentials.json|GMAIL 認證檔位置|
|GMAIL_TOKEN_PICKLE|cert/gmail_token.pickle|GMAIL 授權檔位置|



### Gmail 下載附件

```console
    python app.py -s gmail # 下載所有的附件
```
### Ad Manager 提取所有廣告訂單

```console
    python app.py -s admanager  -p '{"order_id": 廣告訂單ID }'   # 抓出條件是ORDER_ID的廣告訂單
```

### 簡單的git指令

    <a href="GITFIRST.md">由此去</a>