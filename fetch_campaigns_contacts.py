from library.select_account_from_file import main as select_account_from_file
import pandas as pd
import requests
import os
from test_platform import *
import json
from library.async_get_contacts import *
from library.get_local_timezone import convertTimeToUTC
from library.mailjet_exclude import *

ROOT_URL = "https://api.mailjet.com/v3/REST"
# Only fetch 1000 campaigns

OUTPUT = "campaign_nondeliveries-spam/" # where to output the result
LIMIT = 1000

# Loading api data
with open('api.json') as f:
  data = json.load(f)

exit_code, selected_accounts = select_account_from_file(data)
print(exit_code, selected_accounts)

# Enter Time Range for later fetching campaign recipient data
print("Please enter your datetime (yyyy-mm-dd hh:mm)")
d_from_input = input("%-7s" %("From: "))
from_datetime = convertTimeToUTC(d_from_input) 

d_to_input = input("%-7s" %("To: "))
to_datetime = convertTimeToUTC(d_to_input)
campaign_endpoint = "/campaign" + "?FromTS=%s&ToTS=%s&Limit=1000&CampaignType=2" %(from_datetime, to_datetime)

if exit_code == -1:
    exit(-1)

for account in selected_accounts:
    print("Processing account ", account)
    api_pair = data[account].split(":")
    api_key, api_secret = api_pair[0],api_pair[1]    

    res = requests.get(ROOT_URL+campaign_endpoint, auth=(api_key,api_secret))

    f_name = OUTPUT + account + '_non_deliveries+spams.xlsx'

    all_nondelivery = pd.DataFrame()

    campaign_ids = []
    for campaign in res.json()["Data"]:
        campaign_ids.append(campaign["ID"])
    print("Found these campaigns: ", campaign_ids)

    if len(campaign_ids) == 0:
        print("No campaigns were found/sent. Program moves on to the next account!")
        continue # skip to other account since no campaigns found in the current account

    with pd.ExcelWriter(f_name) as writer:
        for campaign_id in campaign_ids:
            ### Fetching the contacts    
            s = requests.Session()
            a = requests.adapters.HTTPAdapter(max_retries=3)
            print("Fetching data of the campaign_id %s .." %(campaign_id))
            s.mount("https://", a)    
            url = "https://api.mailjet.com/v3/REST/contact" + "?Campaign=%s" %(campaign_id) + "&countOnly=1"
            res  = s.get(url, auth=(api_key, api_secret))
            if res.status_code != 200:
                print("Something went wrong. ")
                print("Response status code: ", res.status_code)
                print("Program terminated!")
                exit()
            else:
                no_calls = res.json()['Count']
                
            downloaded_contact_list = asyncio.run(getContacts(no_calls, api_key, api_secret, campaign_id))
            
            contacts = []
            for i in range(len(downloaded_contact_list)):
                contacts += downloaded_contact_list[i]

            s = requests.Session()
            a = requests.adapters.HTTPAdapter(max_retries=3)
            s.mount("https://", a)    
            url = "https://api.mailjet.com/v3/REST/message" + "?Campaign=%s" %(campaign_id) + "&countOnly=1"
            res  = s.get(url, auth=(api_key, api_secret))    
            if res.status_code != 200:
                print("Something went wrong. ")
                print("Response status code: ", res.status_code)
                print("Program terminated!")
                exit()
            else:
                no_calls = res.json()['Count']        

            downloaded_message_list = asyncio.run(getRecipients(no_calls, api_key, api_secret, campaign_id))    
            messages = []
            for i in range(len(downloaded_message_list)):
                messages += downloaded_message_list[i]    

            contact_list = pd.DataFrame(data=contacts) 
            message_list = pd.DataFrame(data=messages)
            contact_list["ID"] = contact_list["ID"].astype(int)
            message_list["ContactID"] = message_list["ContactID"].astype(int)
            contact_list = contact_list.rename(columns={"ID":"ContactID"})
            contacts  = message_list.merge(contact_list, on="ContactID", indicator=True)    
            df = pd.DataFrame()    
            
            softbounced = contacts[contacts["Status"]=="softbounced"]
            hardbounced = contacts[contacts["Status"]=="hardbounced"]
            blocked     = contacts[contacts["Status"]=="blocked"]
            deferred    = contacts[contacts["Status"]=="deferred"] 
            spam        = contacts[contacts["Status"]=="spam"]

            df = df.append(softbounced)
            df = df.append(hardbounced)
            df = df.append(blocked)
            df = df.append(deferred)
            df = df.append(spam)


            df = df[["Email", "Status"]].drop_duplicates()

            print("There are %s softbounced, %s hardbounced, %s blocked, %s deferred, %s spam" \
                        %(len(softbounced), len(hardbounced), len(blocked), len(deferred), len(spam)))
            
            print("In total, the number of nondeliveries and spams is ", len(df))
            try:
                df.to_excel(writer,sheet_name=str(campaign_id),index=False)
            except Exception:
                print("There is a file with the same name: %s is opened!" %(f_name))
                print("Turn it off and move it somewhere or rename it if you want keep!")
                input("Type any key and press enter to continue saving..")
                df.to_excel(writer,sheet_name=str(campaign_id),index=False)

            print("-------------------------")
            all_nondelivery = all_nondelivery.append(df)

        all_nondelivery.to_excel(writer,sheet_name="all_nondelivery",index=False)
        print("Done writing to excel file!")
        print("Starting to put to the exclusion list")
        exclude_contacts(all_nondelivery, api_key, api_secret)
        print("Done put the contacts to the exclusion list")        
        
    print("Processing account", account, "is Done")
    print("------------------------------------------------------------------------------")
            
            
