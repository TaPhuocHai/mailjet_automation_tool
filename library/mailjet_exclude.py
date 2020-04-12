#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import requests


EMAIL_HEADER = 'Email'


def exclude_contacts(data, api_key, api_secret):
    serializedData = data[EMAIL_HEADER].tolist()

    theContacts = []

    for contact in serializedData:
        theContacts.append(
            {
            "Email": contact,
            "IsExcludedFromCampaigns": True
            })
 
    data = {
        'Contacts': theContacts        
    }
    url = "https://api.mailjet.com/v3/REST/contact/managemanycontacts"
    result = requests.post(url, json=data, auth=(api_key, api_secret))
    
    if result.status_code == 200 or result.status_code == 201:
        return 1
    else:
        return -1
