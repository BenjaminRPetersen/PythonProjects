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
customObjectID="58"

########################################################
# GET ID OF ELOQUA FIELDS TO SYNC BETA.SAM DATA
########################################################
response = s.get(
    'https://secure.p03.eloqua.com/api/REST/2.0/assets/customObject/'+customObjectID+'?limit=1',
                        auth=(client_name+'\\'+username, password)
    )
customObjectFields = pd.json_normalize(response.json()['fields'])
uiLinkID = customObjectFields['id'][customObjectFields[customObjectFields.name == 'uiLink'].index[0]]
primary_lastNameID = customObjectFields['id'][customObjectFields[customObjectFields.name == 'primary_lastName'].index[0]]
primary_firstNameID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'primary_firstName'].index[0]]
primary_titleID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'primary_title'].index[0]]
primary_phoneID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'primary_phone'].index[0]]
primary_emailID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'primary_email'].index[0]]
stateID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'officeAddress.state'].index[0]]
cityID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'officeAddress.city'].index[0]]
zipID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'officeAddress.zipcode'].index[0]]
activeID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'active'].index[0]]
classificationCodeID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'classificationCode'].index[0]]
naicsCodeID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'naicsCode'].index[0]]
archiveDateID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'archiveDate'].index[0]]
archiveTypeID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'archiveType'].index[0]]
baseTypeID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'baseType'].index[0]]
typeID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'type'].index[0]]
postedDateID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'postedDate'].index[0]]
officeID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'office'].index[0]]
subTierID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'subTier'].index[0]]
departmentID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'department'].index[0]]
solicitationNumberID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'solicitationNumber'].index[0]]
titleID= customObjectFields['id'][customObjectFields[customObjectFields.name == 'title'].index[0]]

########################################################
# CREATE IMPORT DEFINITION
########################################################
payload = {
  "name": "Beta.Sam Opportunities",
  "mapDataCards": "true",
  "mapDataCardsEntityField": "{{Contact.Field(C_EmailAddress)}}",
  "mapDataCardsSourceField": "primary_email",
  "mapDataCardsEntityType": "Contact",
  "mapDataCardsCaseSensitiveMatch": "false",
  "updateRule": "always",
  "fields": {
      "solicitationNumber": "{{CustomObject["+customObjectID+"].Field["+solicitationNumberID+"]}}",
      "primary_email": "{{CustomObject["+customObjectID+"].Field["+primary_emailID+"]}}",
      "primary_firstName": "{{CustomObject["+customObjectID+"].Field["+primary_firstNameID+"]}}",
      "primary_lastName": "{{CustomObject["+customObjectID+"].Field["+primary_lastNameID+"]}}",
      "uiLink": "{{CustomObject["+customObjectID+"].Field["+uiLinkID+"]}}",
      "primary_title": "{{CustomObject["+customObjectID+"].Field["+primary_titleID+"]}}",
      "primary_phone": "{{CustomObject["+customObjectID+"].Field["+primary_phoneID+"]}}",
      "officeAddressState": "{{CustomObject["+customObjectID+"].Field["+stateID+"]}}",
      "officeAddressCity": "{{CustomObject["+customObjectID+"].Field["+cityID+"]}}",
      "officeAddressZipcode": "{{CustomObject["+customObjectID+"].Field["+zipID+"]}}",
      "active": "{{CustomObject["+customObjectID+"].Field["+activeID+"]}}",
      "classificationCode": "{{CustomObject["+customObjectID+"].Field["+classificationCodeID+"]}}",
      "naicsCode": "{{CustomObject["+customObjectID+"].Field["+naicsCodeID+"]}}",
      "archiveDate": "{{CustomObject["+customObjectID+"].Field["+archiveDateID+"]}}",
      "archiveType": "{{CustomObject["+customObjectID+"].Field["+archiveTypeID+"]}}",
      "baseType": "{{CustomObject["+customObjectID+"].Field["+baseTypeID+"]}}",
      "type": "{{CustomObject["+customObjectID+"].Field["+typeID+"]}}",
      "postedDate": "{{CustomObject["+customObjectID+"].Field["+postedDateID+"]}}",
      "office": "{{CustomObject["+customObjectID+"].Field["+officeID+"]}}",
      "subTier": "{{CustomObject["+customObjectID+"].Field["+subTierID+"]}}",
      "department": "{{CustomObject["+customObjectID+"].Field["+departmentID+"]}}",
      "title": "{{CustomObject["+customObjectID+"].Field["+titleID+"]}}"
   },
   "identifierFieldName": "solicitationNumber",
   "isSyncTriggeredOnImport" : "true"
}
  
payload = json.dumps(payload)
post = s.post(   
        "https://secure.p03.eloqua.com/api/bulk/2.0/customobjects/"+customObjectID+"/imports",
                            auth=(client_name+'\\'+username, password),
                            data = payload,
                            headers = {'Content-Type':'application/json'}
        )
imp_def_opportunities = json.loads(json.dumps(post.json()))['uri']

print(imp_def_opportunities)
