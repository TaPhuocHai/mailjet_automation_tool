import requests
api_key = ''
api_secret = ''


job_id = "" # edit this to see the log
url = "https://api.mailjet.com/v3/REST/contact/managemanycontacts/%s" %(job_id)
job = requests.get(url, auth=(api_key, api_secret))

print(job.json())
# job.content


url = "https://api.mailjet.com/v3/DATA/batchjob/%s/JSONError/application:json/LAST" %(job_id)
error_file = requests.get(url, auth=(api_key, api_secret))


# print(error_file.json())
f = open("error.json",mode="w+",encoding="utf-8")
f.write(str(error_file.json()))
f.close()