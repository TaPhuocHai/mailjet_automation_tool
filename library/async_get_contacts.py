import asyncio
import aiohttp
import requests


NUMBER_CONCURRENTS = 50

async def fetchContacts(url, session, auth):
    contact_data = []
    async with session.get(url, auth=auth) as response:
        contacts = await response.json()
        for c in contacts['Data']:
            contact_data.append(c)
        return contact_data

async def fetchRecipients(url, session, auth):
    recipients = []
    async with session.get(url, auth=auth) as response:
        contacts = await response.json()
        for c in contacts['Data']:
            recipients.append(c)
        return recipients

async def boundFetchContacts(sem, url, session, auth):
    # Getter function with semaphore
    async with sem:
        return await fetchContacts(url, session, auth)

async def boundFetchRecipients(sem, url, session, auth):
    # Getter function with semaphore
    async with sem:
        return await fetchRecipients(url, session, auth)

async def getContacts(r, API_KEY, API_SECRET, campaign_id):
    """
    Wrapper of async fetch snippet
    """
    # Create the queue of work
    
    LIMIT = 1000
    offset = 0
    generate_url = lambda offset: "https://api.mailjet.com/v3/REST/contact/?limit=" +str(LIMIT) + "&offset=" + str(offset) + "&Campaign=%s" %(campaign_id)
    
    tasks = []
    sem = asyncio.Semaphore(NUMBER_CONCURRENTS) # safe number of concurrent requests
    auth = aiohttp.BasicAuth(API_KEY, API_SECRET)
    conn = aiohttp.TCPConnector(verify_ssl=False)
    timeout = aiohttp.ClientTimeout(total=60,connect=15)
    async with aiohttp.ClientSession(headers={"Connection": "close"}, connector=conn, timeout=timeout) as session:
        
        while offset < r:
            task = asyncio.ensure_future(boundFetchContacts(sem, generate_url(offset),session, auth))
            tasks.append(task)
            offset += LIMIT
        responses = asyncio.gather(*tasks)
        return await responses

async def getRecipients(r, API_KEY, API_SECRET, campaign_id):
    """
    Wrapper of async fetch snippet
    """
    # Create the queue of work
    
    LIMIT = 1000
    offset = 0
    generate_url = lambda offset: "https://api.mailjet.com/v3/REST/message/?limit=" +str(LIMIT) + "&offset=" + str(offset) + "&Campaign=%s" %(campaign_id)
    
    tasks = []
    sem = asyncio.Semaphore(NUMBER_CONCURRENTS) # safe number of concurrent requests
    auth = aiohttp.BasicAuth(API_KEY, API_SECRET)
    conn = aiohttp.TCPConnector(verify_ssl=False)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(headers={"Connection": "close"}, connector=conn,timeout=timeout) as session:
        while offset < r:
            task = asyncio.ensure_future(boundFetchRecipients(sem, generate_url(offset),session, auth))
            tasks.append(task)
            offset += LIMIT
        responses = asyncio.gather(*tasks)
        return await responses

def checkStatus(row):
    if row == False:
        return "sub"
    else: 
        return "unsub"



    
