# Gcloud_Integration整合服務

## 目錄結構：
- admanager: Google Ad Manager串接
- googlesheets: Google Spreadsheet串接
- gmail_attachment: Sendgrid Email串接
- cert: 存放憑證檔案
- app: Package
- adreport: Controller 

## .env 環境參數：

|Key|Value|Desc|
|---|-----|----|
|GOOGLEADS_YAML|cert/googleads.yaml|Ad Manager 設定檔位置|
|DFP_KEY|cert/cw-web-prod-ad-manager.json|Google API Service Account Key|
|SENDGRID_API_KEY||Sendgrid Email Key|
|SHARE_FOLDER||Google Drive共享資料夾ID|
|TEMPLATE_SPREADSHEET_ID. TEMPLATE_SHEET_ID||Google Spreadsheet報表模板|


## 運作流程
Ad Manager下載報表 → 產出Google Spreadsheet報表 → 寄出Email
 

## 執行

```console
    python app.py -s adreport  
```
