#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import json
import requests
api_key = ''
api_secret = ''
LIST_ID = ''

ACTION = "addnoforce" # if you want to sub
# ACTION = "unsub" # if you want to unsub



NAME = ""
EMAIL_HEADER = 'email_address' # change according to your data

class Contact:
    def __init__(self, email='', properties={}):
        self.email = email
        self.properties = properties
    def __repr__(self):
        return self.email


# Create contact by reading csv file
if NAME.find(".xlsx") != -1:
    data = pd.read_excel(NAME)
elif NAME.find(".csv") != -1:
    data = pd.read_csv(NAME)
else:
    print("File error")
    exit(-1)

data = data.astype('str')



serializedData = []
for idx, row in data.iterrows():     
    raw_contact = row.to_dict()
    d = dict()
        
    for prop, val in raw_contact.copy().items():
        if pd.isna(val) or str(val) == 'nan':
            raw_contact[prop] = ' '
            # raw_contact.pop(prop)
      

    email = raw_contact.pop(EMAIL_HEADER)
    contact = Contact(email=email, properties=raw_contact)    
    serializedData.append(contact)

theContacts = []




for contact in serializedData:
    theContacts.append(
        {
          "Email": contact.email,
          "IsExcludedFromCampaigns": "false",
          "Properties": contact.properties
        })

# print(theContacts)
# json.loads(theContacts)
# exit(-1)
# exit()
LIMIT = 10000
if len(theContacts) > 10000:
    begin = 0
    end =  LIMIT #len(theContacts)
    
    while end < len(theContacts):
        print(len(theContacts[begin:end]))
        data = {
            'Action': ACTION,
            'Contacts': theContacts[begin:end],
        }
        url = "https://api.mailjet.com/v3/REST/contactslist/%s/managemanycontacts" % (LIST_ID)
        result = requests.post(url, json=data, auth=(api_key, api_secret))

        if result.status_code == 200 or result.status_code == 201:
            print (result.status_code)
            print (result.json())
        else:
            print(result.status_code)
            print('error with: ', result)

        print("Done request")
        print(begin, end)
        begin = end
        end  += LIMIT

    print(len(theContacts[begin:end]))
    data = {
        'Action': ACTION,
        'Contacts': theContacts[begin:end],
    }
    url = "https://api.mailjet.com/v3/REST/contactslist/%s/managemanycontacts" % (LIST_ID)
    result = requests.post(url, json=data, auth=(api_key, api_secret))

    if result.status_code == 200 or result.status_code == 201:
        print (result.status_code)
        print (result.json())
    else:
        print(result.status_code)
        print('error with: ', result)

    print("Done request")
    print(begin, end)    

# print(begin,end)
else:
        
    data = {
        'Action': ACTION,
        'Contacts': theContacts,
    }
    url = "https://api.mailjet.com/v3/REST/contactslist/%s/managemanycontacts" % (LIST_ID)
    result = requests.post(url, json=data, auth=(api_key, api_secret))

    if result.status_code == 200 or result.status_code == 201:
        print (result.status_code)
        print (result.json())
    else:
        print(result.status_code)
        print('error with: ', result)
