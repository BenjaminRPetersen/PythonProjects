########################################################
# TITLE: BETA.SAM.GOV OPPORTUNITY API TO ELOQUA API
# AUTHOR: BENJAMIN PETERSEN
# LAST MODIFIED DATE: 12-22-2020
#
########################################################

########################################################
# PACKAGES
########################################################

import requests # FOR API REQUESTS
import pandas as pd # FOR REORGANIZING DATA
import json # FORMATTING AND DECOUPLING JSON
import datetime # FOR DATETIME TRANSFORMATIONS
import time # TO PAUSE PROGRAM
import os # TO GET ENVIRONMENT VARIABLES
import logging # LOGGING FOR API CONNECTION
from requests.adapters import HTTPAdapter # TO RESET CONNECTION
from requests.packages.urllib3.util.retry import Retry # RETRY CONNECTIONS
logging.basicConfig(level=logging.DEBUG) # LOGGING SETTINGS


########################################################
# PRESET VARIABLES
########################################################

s = requests.Session() # SET SESSION
retries = Retry(total=5,
                backoff_factor=1,
                status_forcelist=[ 502, 503, 504 ]) # RETRY AFTER 500 ERRORS
s.mount('http://',
        HTTPAdapter(max_retries=retries)) # SET RETRY LIMIT
today = datetime.datetime.today() # GET TODAY'S DATE
yesterday = today - datetime.timedelta(days = 1) # GET YESTERDAY'S DATE
yesterday = yesterday.strftime("%m/%d/%Y") # FORMAT YESTERDAY'S DATE
temp_stage = pd.DataFrame({'title':[],
                     'solicitationNumber':[],
                     'department':[],
                     'subTier':[],
                     'office':[],
                     'postedDate':[],
                     'type':[],
                     'baseType':[],
                     'archiveType':[],
                     'archiveDate':[],
                     'naicsCode':[],
                     'classificationCode':[],
                     'active':[],
                     'officeAddress.zipcode':[],
                     'officeAddress.city':[],
                     'officeAddress.state':[],
                     'uiLink':[]}) # BUILD TEMPORARY STAGING TABLE FOR DATA
temp = pd.DataFrame({'title':[],
                     'solicitationNumber':[],
                     'department':[],
                     'subTier':[],
                     'office':[],
                     'postedDate':[],
                     'type':[],
                     'baseType':[],
                     'archiveType':[],
                     'archiveDate':[],
                     'naicsCode':[],
                     'classificationCode':[],
                     'active':[],
                     'officeAddress.zipcode':[],
                     'officeAddress.city':[],
                     'officeAddress.state':[],
                     'primary_email':[],
                     'primary_phone':[],
                     'primary_title':[],
                     'primary_firstName':[],
                     'primary_lastName':[],
                     'secondary_email':[],
                     'secondary_phone':[],
                     'secondary_title':[],
                     'secondary_firstName':[],
                     'secondary_lastName':[],
                     'uiLink':[]}) # SET AN EMPTY DATAFRAME FOR OPPORTUNITIES (IF DATA ISN'T AVAILABLE THEN THIS WILL STILL PROVIDE THE COLUMN FOR THE DATA UPLOAD)
temp_contact = pd.DataFrame({'solicitationNumber':[],
                             'email':[],
                             'phone':[],
                             'title':[],
                             'fullName':[],
                             'firstName':[],
                             'lastName':[],
                             'type':[]}) # SET AN EMPTY DATA FRAME FOR CONTACTS
api_key = os.environ.get('BETA_SAM_API_KEY') # GET THE API KEY FROM THE ENVIRONMENT VARIABLES
client_name = os.environ.get('ELOQUA_SANDBOX') # SET ELOQUA CLIENT NAME
username= os.environ.get('ELOQUA_SANDBOX_USERNAME') # SET ELOQUA USERNAME
password= os.environ.get('ELOQUA_SANDBOX_PASSWORD') # SET ELOQUA PASSWORD
import_definition_contacts = "https://secure.p03.eloqua.com/api/bulk/2.0/contacts/imports/25550/data" # SET URI FOR CONTACT IMPORT DEFINITION
import_definition_opportunities = "https://secure.p03.eloqua.com/api/bulk/2.0//customObjects/58/imports/25549/data" # SET URL FOR OPPORUTNITIES IMPORT DEFINITION
customObjectID = "58" # SET CUSTOM OBJECT ID (THIS IS THE ID OF THE OPPORTUNITIES OBJECT)

########################################################
# LOAD STAGE 1: SOURCES SOUGHT
########################################################
one = s.get('https://api.sam.gov/prod/opportunities/v1/search?limit=1000&api_key='+api_key+'&postedFrom='
                        +yesterday
                        +'&postedTo='
                        +yesterday
                        +'&ptype=r&active=Yes') # API REQUEST
one = one.json() # PULL JSON
pointOfContact = pd.json_normalize(one['opportunitiesData'],
                                   record_path='pointOfContact',
                                   meta=['solicitationNumber']) # NORMALIZE CONTACTS INTO DATAFRAME
pointOfContact = pd.concat([pointOfContact[pointOfContact.columns.intersection(temp_contact.columns)],temp_contact]
                           , axis=0
                           , ignore_index=True).astype(str) # CREATE POINT OF CONTACT DATAFRAME WITH ALL AVAILABLE COLUMNS
pointOfContact['firstName'] = pointOfContact.fullName.str.split(" ").str[0].str.title() # PROPERCASE FIRST NAME
pointOfContact['lastName'] = pointOfContact.fullName.str.split(" ").str[-1].str.title() # PROPERCASE LAST NAME
pointOfContact['valid'] = pointOfContact['email'].str.contains("@") # CHECK FOR VALID EMAIL ADDRESS
pointOfContact = pointOfContact[pointOfContact['valid'] == True]

primary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'primary'] # GET PRIMARY CONTACT BY OPPORTUNITY
primary.columns = ['primary_' + str(col) for col in primary.columns] # ADD PRIMARY IDENTIFIER TO FIELDS

secondary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'secondary'] # GET SECONDARY CONTACT BY OPPORTUNITY
secondary.columns = ['secondary_' + str(col) for col in secondary.columns] # ADD SECONDARY IDENTIFIER TO FIELDS

sources_sought = pd.json_normalize(one, 'opportunitiesData') # NORMALIZE SOURCES SOUGHT DATA
sources_sought = pd.concat([sources_sought[sources_sought.columns.intersection(temp_stage.columns)],temp_stage]
                           , axis=0
                           , ignore_index=True) # JOIN TO TEMPORARY TABLE TO ENSURE CONSISTENCY ACROSS DATA SOURCES
sources_sought = sources_sought.merge(primary, how='inner', left_on='solicitationNumber', right_on='primary_solicitationNumber') # ATTACH PRIMARY CONTACT TO OPPORTUNITY
sources_sought = sources_sought.merge(secondary, how='left', left_on='solicitationNumber', right_on='secondary_solicitationNumber') # ATTACH SECONDARY CONTACT TO OPPORTUNITY
sources_sought = pd.concat([sources_sought[sources_sought.columns.intersection(temp.columns)],temp]
                           , axis=0
                           , ignore_index=True) # JOIN TO TEMPORARY TABLE TO ENSURE CONSISTENCY ACROSS DATA SOURCES

print('stage 1: sources_sought loaded')

########################################################
# LOAD STAGE 2: PRE SOLICITATION (SEE COMMENTS FROM STAGE 1)
########################################################

two = s.get('https://api.sam.gov/prod/opportunities/v1/search?limit=1000&api_key='+api_key+'&postedFrom='
                        +yesterday
                        +'&postedTo='
                        +yesterday
                        +'&ptype=p&active=Yes')
two = two.json()
pointOfContact = pd.json_normalize(two['opportunitiesData'], record_path='pointOfContact', meta=['solicitationNumber'])
pointOfContact = pd.concat([pointOfContact[pointOfContact.columns.intersection(temp_contact.columns)],temp_contact]
                           , axis=0
                           , ignore_index=True).astype(str)
pointOfContact['firstName'] = pointOfContact.fullName.str.split(" ").str[0].str.title()
pointOfContact['lastName'] = pointOfContact.fullName.str.split(" ").str[-1].str.title()
pointOfContact['valid'] = pointOfContact['email'].str.contains("@")
pointOfContact = pointOfContact[pointOfContact['valid'] == True]

primary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'primary']
primary.columns = ['primary_' + str(col) for col in primary.columns]

secondary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'secondary']
secondary.columns = ['secondary_' + str(col) for col in secondary.columns]

pre_solicitation = pd.json_normalize(two, 'opportunitiesData')
pre_solicitation = pd.concat([pre_solicitation[pre_solicitation.columns.intersection(temp_stage.columns)],temp_stage]
                           , axis=0
                           , ignore_index=True).astype(str)
pre_solicitation = pre_solicitation.merge(primary, how='inner', left_on='solicitationNumber', right_on='primary_solicitationNumber')
pre_solicitation = pre_solicitation.merge(secondary, how='left', left_on='solicitationNumber', right_on='secondary_solicitationNumber')
pre_solicitation = pd.concat([pre_solicitation[pre_solicitation.columns.intersection(temp.columns)],temp]
                           , axis=0
                           , ignore_index=True).astype(str)

print('stage 2: pre_solicitation loaded')

########################################################
# LOAD STAGE 3: SOLICITATION (SEE COMMENTS FROM STAGE 1)
########################################################

three = s.get('https://api.sam.gov/prod/opportunities/v1/search?limit=1000&api_key='+api_key+'&postedFrom='
                        +yesterday
                        +'&postedTo='
                        +yesterday
                        +'&ptype=o&active=Yes')
three = three.json()
pointOfContact = pd.json_normalize(three['opportunitiesData'], record_path='pointOfContact', meta=['solicitationNumber'])
pointOfContact = pd.concat([pointOfContact[pointOfContact.columns.intersection(temp_contact.columns)],temp_contact]
                           , axis=0
                           , ignore_index=True).astype(str)
pointOfContact['firstName'] = pointOfContact.fullName.str.split(" ").str[0].str.title()
pointOfContact['lastName'] = pointOfContact.fullName.str.split(" ").str[-1].str.title()
pointOfContact['valid'] = pointOfContact['email'].str.contains("@")
pointOfContact = pointOfContact[pointOfContact['valid'] == True]

primary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'primary']
primary.columns = ['primary_' + str(col) for col in primary.columns]

secondary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'secondary']
secondary.columns = ['secondary_' + str(col) for col in secondary.columns]

solicitation = pd.json_normalize(three, 'opportunitiesData')
solicitation = pd.concat([solicitation[solicitation.columns.intersection(temp_stage.columns)],temp_stage]
                           , axis=0
                           , ignore_index=True).astype(str)
solicitation = solicitation.merge(primary, how='inner', left_on='solicitationNumber', right_on='primary_solicitationNumber')
solicitation = solicitation.merge(secondary, how='left', left_on='solicitationNumber', right_on='secondary_solicitationNumber')
solicitation = pd.concat([solicitation[solicitation.columns.intersection(temp.columns)],temp]
                           , axis=0
                           , ignore_index=True).astype(str)

print('stage 3: solicitation loaded')

########################################################
# LOAD STAGE 4: COMBINED SYNOPSIS / SOLICITATION (SEE COMMENTS FROM STAGE 1)
########################################################

foura = s.get('https://api.sam.gov/prod/opportunities/v1/search?limit=1000&api_key='+api_key+'&postedFrom='
                        +yesterday
                        +'&postedTo='
                        +yesterday
                        +'&ptype=k&active=Yes')
foura = foura.json()
pointOfContact = pd.json_normalize(foura['opportunitiesData'], record_path='pointOfContact', meta=['solicitationNumber'])
pointOfContact = pd.concat([pointOfContact[pointOfContact.columns.intersection(temp_contact.columns)],temp_contact]
                           , axis=0
                           , ignore_index=True).astype(str)
pointOfContact['firstName'] = pointOfContact.fullName.str.split(" ").str[0].str.title()
pointOfContact['lastName'] = pointOfContact.fullName.str.split(" ").str[-1].str.title()
pointOfContact['valid'] = pointOfContact['email'].str.contains("@")
pointOfContact = pointOfContact[pointOfContact['valid'] == True]

primary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'primary']
primary.columns = ['primary_' + str(col) for col in primary.columns]

secondary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'secondary']
secondary.columns = ['secondary_' + str(col) for col in secondary.columns]

combined_synopsis_solicitation = pd.json_normalize(foura, 'opportunitiesData')
combined_synopsis_solicitation = pd.concat([combined_synopsis_solicitation[combined_synopsis_solicitation.columns.intersection(temp_stage.columns)],temp_stage]
                           , axis=0
                           , ignore_index=True).astype(str)
combined_synopsis_solicitation = combined_synopsis_solicitation.merge(primary, how='inner', left_on='solicitationNumber', right_on='primary_solicitationNumber')
combined_synopsis_solicitation = combined_synopsis_solicitation.merge(secondary, how='left', left_on='solicitationNumber', right_on='secondary_solicitationNumber')
combined_synopsis_solicitation = pd.concat([combined_synopsis_solicitation[combined_synopsis_solicitation.columns.intersection(temp.columns)],temp]
                           , axis=0
                           , ignore_index=True).astype(str)

print('stage 4: combined_synopsis_solicitation loaded')

########################################################
# LOAD STAGE 4: SPECIAL NOTICE (SEE COMMENTS FROM STAGE 1)
########################################################

fourb = s.get('https://api.sam.gov/prod/opportunities/v1/search?limit=1000&api_key='+api_key+'&postedFrom='
                        +yesterday
                        +'&postedTo='
                        +yesterday
                        +'&ptype=s&active=Yes')
fourb = fourb.json()
pointOfContact = pd.json_normalize(fourb['opportunitiesData'], record_path='pointOfContact', meta=['solicitationNumber'])
pointOfContact = pd.concat([pointOfContact[pointOfContact.columns.intersection(temp_contact.columns)],temp_contact]
                           , axis=0
                           , ignore_index=True).astype(str)
pointOfContact['firstName'] = pointOfContact.fullName.str.split(" ").str[0].str.title()
pointOfContact['lastName'] = pointOfContact.fullName.str.split(" ").str[-1].str.title()
pointOfContact['valid'] = pointOfContact['email'].str.contains("@")
pointOfContact = pointOfContact[pointOfContact['valid'] == True]

primary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'primary']
primary.columns = ['primary_' + str(col) for col in primary.columns]

secondary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'secondary']
secondary.columns = ['secondary_' + str(col) for col in secondary.columns]

special_notice = pd.json_normalize(fourb, 'opportunitiesData')
special_notice = pd.concat([special_notice[special_notice.columns.intersection(temp_stage.columns)],temp_stage]
                           , axis=0
                           , ignore_index=True).astype(str)
special_notice = special_notice.merge(primary, how='inner', left_on='solicitationNumber', right_on='primary_solicitationNumber')
special_notice = special_notice.merge(secondary, how='left', left_on='solicitationNumber', right_on='secondary_solicitationNumber')
special_notice = pd.concat([special_notice[special_notice.columns.intersection(temp.columns)],temp]
                           , axis=0
                           , ignore_index=True).astype(str)

print('stage 4: special_notice loaded')

########################################################
# LOAD STAGE 4: JUSTIFICATION (SEE COMMENTS FROM STAGE 1)
########################################################

fourc = s.get('https://api.sam.gov/prod/opportunities/v1/search?limit=1000&api_key='+api_key+'&postedFrom='
                        +yesterday
                        +'&postedTo='
                        +yesterday
                        +'&ptype=u&active=Yes')
fourc = fourc.json()
pointOfContact = pd.json_normalize(fourc['opportunitiesData'], record_path='pointOfContact', meta=['solicitationNumber'])
pointOfContact = pd.concat([pointOfContact[pointOfContact.columns.intersection(temp_contact.columns)],temp_contact]
                           , axis=0
                           , ignore_index=True).astype(str)
pointOfContact['firstName'] = pointOfContact.fullName.str.split(" ").str[0].str.title()
pointOfContact['lastName'] = pointOfContact.fullName.str.split(" ").str[-1].str.title()
pointOfContact['valid'] = pointOfContact['email'].str.contains("@")
pointOfContact = pointOfContact[pointOfContact['valid'] == True]

primary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'primary']
primary.columns = ['primary_' + str(col) for col in primary.columns]

secondary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'secondary']
secondary.columns = ['secondary_' + str(col) for col in secondary.columns]

justification = pd.json_normalize(fourc, 'opportunitiesData')
justification = pd.concat([justification[justification.columns.intersection(temp_stage.columns)],temp_stage]
                           , axis=0
                           , ignore_index=True).astype(str)
justification = justification.merge(primary, how='inner', left_on='solicitationNumber', right_on='primary_solicitationNumber')
justification = justification.merge(secondary, how='left', left_on='solicitationNumber', right_on='secondary_solicitationNumber')
justification = pd.concat([justification[justification.columns.intersection(temp.columns)],temp]
                           , axis=0
                           , ignore_index=True).astype(str)

print('stage 4: justification loaded')

########################################################
# LOAD STAGE 5: AWARD NOTICE (SEE COMMENTS FROM STAGE 1)
########################################################

five = s.get('https://api.sam.gov/prod/opportunities/v1/search?limit=1000&api_key='+api_key+'&postedFrom='
                        +yesterday
                        +'&postedTo='
                        +yesterday
                        +'&ptype=a&active=Yes')
five = five.json()
pointOfContact = pd.json_normalize(five['opportunitiesData'], record_path='pointOfContact', meta=['solicitationNumber'])
pointOfContact = pd.concat([pointOfContact[pointOfContact.columns.intersection(temp_contact.columns)],temp_contact]
                           , axis=0
                           , ignore_index=True).astype(str)
pointOfContact['firstName'] = pointOfContact.fullName.str.split(" ").str[0].str.title()
pointOfContact['lastName'] = pointOfContact.fullName.str.split(" ").str[-1].str.title()
pointOfContact['valid'] = pointOfContact['email'].str.contains("@")
pointOfContact = pointOfContact[pointOfContact['valid'] == True]

primary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'primary']
primary.columns = ['primary_' + str(col) for col in primary.columns]

secondary = pointOfContact[['solicitationNumber','email', 'phone', 'title', 'firstName', 'lastName']][pointOfContact.type == 'secondary']
secondary.columns = ['secondary_' + str(col) for col in secondary.columns]

award_notice = pd.json_normalize(five, 'opportunitiesData')
award_notice = pd.concat([award_notice[award_notice.columns.intersection(temp_stage.columns)],temp_stage]
                           , axis=0
                           , ignore_index=True).astype(str)
award_notice = award_notice.merge(primary, how='inner', left_on='solicitationNumber', right_on='primary_solicitationNumber')
award_notice = award_notice.merge(secondary, how='left', left_on='solicitationNumber', right_on='secondary_solicitationNumber')
award_notice = pd.concat([award_notice[award_notice.columns.intersection(temp.columns)],temp]
                           , axis=0
                           , ignore_index=True).astype(str)

print('stage 5: award_notice loaded')

########################################################
# COMBINE ALL BETA.SAM DATA AND GET A UNIQUE LIST OF CONTACTS
########################################################

sam = pd.concat([sources_sought,
                 pre_solicitation,
                 solicitation,
                 combined_synopsis_solicitation,
                 special_notice,
                 justification,
                 award_notice]) # ALL BETA.SAM DATA
beta_contacts = sam[['primary_email',
                     'primary_phone',
                     'primary_title',
                     'primary_firstName',
                     'primary_lastName',
                     'officeAddress.zipcode',
                     'officeAddress.city',
                     'officeAddress.state',
                     'department',
                     'subTier']].drop_duplicates(subset = ['primary_email']) # UNIQUE CONTACTS BASED ON EMAILS

########################################################
# BULK IMPORT CONTACTS INTO ELOQUA
########################################################
beta_contacts.rename(columns={'primary_email':'EmailAddress',
                              'primary_phone':'BusinessPhone',
                              'primary_title':'Title',
                              'primary_firstName':'FirstName',
                              'primary_lastName':'LastName',
                              'officeAddress.zipcode':'ZiporPostalCode',
                              'officeAddress.city':'City',
                              'officeAddress.state':'StateorProvince'},
                     inplace=True) # RENAME COLUMNS TO MATCH IMPORT DEFINITION
result = beta_contacts.to_json(orient="records") # FORMAT INTO JSON STEP 1
parsed = json.loads(result) # FORMAT INTO JSON STEP 2
payload = json.dumps(parsed) # CREATE PAYLOAD
post = s.post(import_definition_contacts,
              auth=(client_name+'\\'+username, password),
              data = payload,
              headers = {'Content-Type':'application/json'}
        ) # PASS CONTACTS TO CONTACT OBJECT
print(str(len(beta_contacts)),' Beta.Sam Contacts Loaded') # PRINT NUMBER OF CONTACTS LOADED

########################################################
# GET CUSTOM OBJECT FIELDS
########################################################

response = s.get(
    'https://secure.p03.eloqua.com/api/REST/2.0/assets/customObject/'+customObjectID+'?limit=1',
                        auth=(client_name+'\\'+username, password)
    ) # GET CUSTOM OBJECT FROM API
customObjectFields = pd.json_normalize(response.json()['fields']) # PUT CUSTOM OBJECT FIELDS INTO DATAFRAME

########################################################
# GET ID OF ELOQUA FIELDS TO MAP TO BETA.SAM DATA
########################################################
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
# BULK IMPORT BETA.SAM OPPORTUNITIES INTO ELOQUA
########################################################
sam.rename(columns={'officeAddress.state':'officeAddressState',
                    'officeAddress.city':'officeAddressCity',
                    'officeAddress.zipcode':'officeAddressZipcode'},
           inplace=True) # RENAME COLUMNS TO MATCH IMPORT DEFINITION
result = sam.to_json(orient="records") # REFORMAT TO JSON STEP 1
parsed = json.loads(result) # REFORMAT TO JSON STEP 2
payload = json.dumps(parsed) # CREATE PAYLOAD
post = s.post(   
        import_definition_opportunities,
        auth=(client_name+'\\'+username, password),
        data = payload,
        headers = {'Content-Type':'application/json'}
        ) # POST TO CUSTOM DATA OBJECT
print(str(len(sam)),' Beta.Sam Opportunities Loaded')
