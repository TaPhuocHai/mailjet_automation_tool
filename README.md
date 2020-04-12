# mailjet_automation_tool

## How To Install
1) Python
Ensure you have Python version 3.4 or later. Kindly visit https://www.python.org/ and download Python version 3 on your system.

2) Clone or simply download the code

3) Install virtual environment package (Highly recommend but not 100% necessary)
> python -m pip install --user virtualenv
Then create an isolated virtual enviroment
> python -m virtualenv ematic_env

4) Activate the virtual enviroment (You can skip this if you don't do step 3)
> source ematic_env/bin/activate

5) Install dependencies for our program
> pip install -r requirements.txt

## How to Use
First, we need to provide credentials. Kindly open the file .env_example, copy and paste in to a new file ".env" at the same root directory. Then please change the information accordingly.

You also need to have 2 folder: input and output.

In the input folder, place the data in csv or excel format (xlsx, xls). Then open the file mailjet_automation.py, change the constant NEW_DATA to the name of the new data file.

Open our VPN connection and run 
> python mailjet_automation.py

## Update Logs:

- 2020-04-08:
  - Autoconnection to VPN: The programs will automatically connect VPN for you.

    All you need to do is to configure the correct name of your VPN connection. 
    Please change the value of the constant VPN in the .env file to the correct name.


- 2020-04-12:
  - Add a script to fetch nondeliveries + spams emails per sent campaign

## Features:
### Fetch nondeliveries + spams emails per sent campaign
This script fetch_campaigns_contacts.py will help you download all nondelivered contacts and those contacts mark your sent campaign message as spam.
Main functionalities:
- Multiple select different accounts via console style interaction
- Allow fetching contacts from sent campaigns based on time range using your local time
- The nondelivered + spam contacts automatically go to the exclusion list

Be sure to install dependencies 
> pip install -r requirements.txt
Steps to use:
1. Create a copy of the file api-example.json, change it to api.json
2. Edit the file api.json: 
  Place the account name as key and the api key and api secret as value. You must place a colon to seperate api key and api secret
3. Run the script using command:
> python fetch_campaigns_contacts.py

The scripts will give you the file(s) with format:
<your account stored in the api.json file>_deliveries+spams.xlsx
Each file might contains:
  - sheet(s) named after sent_campaign_id, which contains all non-delivered contacts along with contacts mark your sent campaign message as spam. 
  - "all_nondelivery" sheet contains the combination of all non-delivered contacts and spams

