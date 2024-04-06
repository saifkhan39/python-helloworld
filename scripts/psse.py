#%%
import requests
import pandas as pd
import numpy as np
from base64 import b64encode
from requests_pkcs12 import get
from bs4 import BeautifulSoup
from zipfile import ZipFile
from io import BytesIO
from ercot_config import pfx_path, pfx_password

#function read particular file categary and concatenate csvs
def compile_files(zfile, substring: str):
    csvs = zfile.infolist()
    df = pd.concat([pd.read_csv(zfile.open(csv.filename))
                         for csv in csvs if substring in csv.filename])
    df = df.rename(columns=lambda x: x.strip())
    return df

#mis scraper function to pull DAM pss/e model and supporting files
def pull_psse(reportTypeId):
    #define url
    url = f'https://mis.ercot.com/misapp/GetReports.do?reportTypeId={reportTypeId}&mimic_duns=1172089695000'
    #get request to report
    r = get(url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    #bytes to string
    html = r.content.decode('utf-8')
    #parse html string
    soup = BeautifulSoup(html, 'html.parser')
    #pull out friendly names and links
    names = [n.text for n in soup.find_all('td', class_='labelOptional_ind')]
    links = [l.get('href') for l in soup.find_all('a', href=True)]
    #combine to df and filter only csvs
    files = pd.DataFrame(list(zip(names, links)), columns=['Names', 'Links'])
    #create download url
    file_url = "https://mis.ercot.com/" + files.loc[0, 'Links']
    print(file_url)
    #make request with certs
    cert_url = get(file_url, pkcs12_filename=pfx_path, pkcs12_password=pfx_password)
    #extract from each daily zip file w/o downloading
    with ZipFile(BytesIO(cert_url.content)) as zfile:      
        #_Gn_ files: Generator data mapping
        gen_data = compile_files(zfile, '_Gn_')        
        #_Sp_ files: Settlement Point data mapping
        sp_data = compile_files(zfile, '_Sp_')

    return gen_data, sp_data

gen, sp = pull_psse(13070)

#deenergized nodes
sp['energization'] = np.where(sp['Status'] == 'Energized', 0, 1)
sp = sp.pivot(index='Settlement Point Name', columns='Hour', values='energization')
sp = sp[(sp.T != 0).any()].reset_index()

## Send teams message
#define webhook url
webhook = 'https://broadreachpower.webhook.office.com/webhookb2/eac87a79-730b-4de6-8622-4dfc066d1f5d@b70b56ae-4144-4806-a9eb-a23d03a69f48/IncomingWebhook/5dc4f7b87e6f4ae5b04a39820d1c6a13/47c07d63-9bdd-4d99-9f71-ac7e7e4b52e9'

#conver sp to csv
sp_csv = sp.to_csv(index=False)

#define the message to send
message = "Here is the CSV file with the data:\n"
attachments = [
    {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": "data.csv",
        "contentBytes": b64encode(sp_csv.encode()).decode()
    }
]
#%%
#send the message with the CSV file attachment to the Teams channel
response = requests.post(
    webhook,
    json={
        "text": message,
        "attachments": attachments
    }
)

# Check the response status code
if response.status_code == 200:
    print("Message sent successfully")
else:
    print("Error sending message:", response.text)
# %%
