########################################################
# TITLE: IMPORT DEFINITION FOR BETA.SAM CONTACTS
# AUTHOR: BENJAMIN PETERSEN
# DATE: 12-22-2020
#
########################################################

########################################################
# PACKAGES
########################################################
import requests
import pandas as pd
import json
import datetime
import time
import os
import logging
from datetime import timezone
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
logging.basicConfig(level=logging.DEBUG)

########################################################
# PRESET VARIABLES
########################################################
s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))
client_name = os.environ.get('ELOQUA_SANDBOX')
username= os.environ.get('ELOQUA_SANDBOX_USERNAME')
password= os.environ.get('ELOQUA_SANDBOX_PASSWORD')

########################################################
# CREATE IMPORT DEFINITION
########################################################
payload = {
  "name": "Beta.Sam Contacts",
  "fields": {
      "FirstName": "{{Contact.Field(C_FirstName)}}",
      "LastName": "{{Contact.Field(C_LastName)}}",
      "EmailAddress": "{{Contact.Field(C_EmailAddress)}}",
      "BusinessPhone": "{{Contact.Field(C_BusPhone)}}",
      "Title": "{{Contact.Field(C_Title)}}",
      "ZiporPostalCode": "{{Contact.Field(C_Zip_Postal)}}",
      "City": "{{Contact.Field(C_City)}}",
      "StateorProvince": "{{Contact.Field(C_State_Prov)}}",
      "department":"{{Contact.Field(C_Company)}}",
      "subTier":"{{Contact.Field(C_Agency_Bureau1)}}",
   },
   "identifierFieldName": "EmailAddress",
   "isSyncTriggeredOnImport" : "true",
   "isUpdatingMultipleMatchedRecords": "false"
}
payload = json.dumps(payload)
post = s.post(   
        "https://secure.p03.eloqua.com/api/bulk/2.0/contacts/imports/",
                            auth=(client_name+'\\'+username, password),
                            data = payload,
                            headers = {'Content-Type':'application/json'}
        )
imp_def_contacts = json.loads(json.dumps(post.json()))['uri']

print(imp_def_contacts)
