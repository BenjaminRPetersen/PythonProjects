# Author: Benjamin Petersen
# Project Description: Sync data from Beta.Sam to Eloqua via API
# Date: 12-22-2020

# Description
Eloqua is a Marketing Automation Tool used by GSA to enhance the customer experience of federal employees, military, vendors, and prospective vendors. In order to reach customers
in a timely manner a python script has been created to run on a 24 hour basis to identify potential opportunities for GSA to assist federal agencies, military, vendors, and
prospective vendors acheive their mission. This respository will only work on a GSA network with valid credentials to Eloqua and an API key to beta.sam.

# Credentials
In order to set up your environment correctly you will have to be on a GSA network with valid credentials to both Eloqua and have a valid beta.sam API key. The credentials for
Eloqua can be stored as account environment variables on your local machine. Click the windows icon -> control panel -> edit the account environment variables -> click "New...".
The variable name is "ELOQUA_SANDBOX_USERNAME" and the variable value is of course your username you are assigned in Eloqua. This step can be repeated to store your client name 
and password for Eloqua's sandbox and production environment as well as the API key that your receive from beta.sam.

# Create Beta.Sam Opportunity Custom Object in Eloqua
In order to store all the data that comes from the Beta.Sam API in Eloqua you will need to create a custom object with the appropriate fields. Please refer to the documentation 
provided by Oracle here: https://docs.oracle.com/en/cloud/saas/marketing/eloqua-user/Help/CustomObjects/Tasks/CreatingCustomObjects.htm

# Creating Import Definitions
Importing data through the Eloqua BULK API will require that you create an import definition for your custom object and for the contact object. The reason behind importing
contacts and custom object data separately is because Eloqua needs to be able to map to contacts from the custom object. (Note: Please don't try using the REST API for this 
process since Eloqua won't map to contacts. I made this mistake early on.) Please refer to the documentation provided by Oracle here: https://docs.oracle.com/en/cloud/saas/marketing/eloqua-rest-api/op-api-bulk-2.0-customobjects-parentid-imports-post.html

# Beta.Sam API Pull
Pulling data through Beta.Sam is limited in the fact that you are only able to pull 1,000 records in a single API request. In order to get around this the data was separated by
the posting type. (Note: pulling data every day will often times result in missing data elements that you will see when government contracts are posted during the busy times of 
the year. For example, sometimes the secondary contact is left empty. By creating empty dataframes and merging them with the data pulled, even if it's empty will circumvent this
issue.) The data is then concatenated into one pandas dataframe and contacts are separated into a pandas dataframe unique to email, as this is what defines a unique contact in 
Eloqua.

# Importing data into Eloqua
If the import definitions have the same name as the columns of the data that was pulled from beta.sam then all you have to do is transform your pandas dataframes into JSON and
pass them through to Eloqua in the master.py file. 
