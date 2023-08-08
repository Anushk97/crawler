import sys

import streamlit as st
import pandas as pd
import numpy as np

st.title('Project Crawler!')

with st.form("my-form", clear_on_submit=True):
    uploaded_file = st.file_uploader("upload file")
    submitted = st.form_submit_button("submit")
#uploaded_file = st.file_uploader("Choose a file")

dataframe = pd.read_excel(uploaded_file)

data = dataframe.drop(labels=0, axis = 0)
data.columns = data.iloc[0]
data = data.drop(labels=1, axis = 0)
dataset = data[500:5500]

st.write(dataset)

#Change this values accordingly
window_size_x = 800 #Enable only when to see the crawler
window_size_y = 800
batch_size = 100
#file_path_source = r'\Users\anushk.farkiya\Downloads\webscraping output - climate data.xlsx'
#path_to_save_output = r"\Users\anushk.farkiya\PycharmProjects\pythonProject\automate\final_output_3.xlsx"
#path_to_blocked_values = r'\Users\anushk.farkiya\PycharmProjects\pythonProject\automate\blocked_batch_3.xlsx'
#ChromeDrive_path = r"D:\Users\anushk.farkiya\Downloads\chromedriver_win32"

#Step 1: Import Libraries
import requests
import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
#import chromedriver_autoinstaller
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import time
#from tqdm import tqdm
import os

#Step 2: Import Data and data transformation
#data = pd.read_excel(file_path_source)
#data = dataframe.drop(labels=0, axis = 0)
#data.columns = data.iloc[0]
#data = data.drop(labels=1, axis = 0)
#data = data[1000:]
output_data = pd.DataFrame()
blocked_data = pd.DataFrame()

total_iterations = len(dataset)
#pbar = tqdm(total=total_iterations, desc='Progress', unit='iteration')
#my_bar = st.progress(0, text="in progress")

#Step 3: Crawler
#service = Service(r"D:\Users\anushk.farkiya\Downloads\chromedriver_win32")
#chromedriver_autoinstaller.install()
#service = os.environ.get(r"D:\Users\anushk.farkiya\Downloads\chromedriver_win32")
#chrome_options = Options()
#option = webdriver.ChromeOptions()
#chrome_options.add_argument('--headless')

def clean_and_split(value):
    cleaned = value.strip()
    split = cleaned.split("\n")
    return [split.strip() for split in split if split.strip()]

end = batch_size

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--headless")

#driver = get_driver()
service = Service()
#options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=service, options=options)
#driver = webdriver.Chrome(ChromeDriverManager().install())
#driver = webdriver.Chrome(options=options)
#driver.get('https://thinkhazard.org/en/')
#st.code(driver.page_source)
batch = 1
total_batches = int(len(dataset)/batch_size)
st.write('total batches', total_batches) 

start = 0
while start < end:
    data_cop = dataset[start:end]
    data_to_merge = data_cop[['Country','Region (HL)','Region (Granular)']]
    #data_to_merge['Region (Granular'] = data_to_merge['Region (Granular)'].astype(str)
    granular = data_cop['Region (Granular)']

    #driver = webdriver.Chrome(options=chrome_options)
    #driver = webdriver.Chrome(ChromeDriverManager().install())
    #driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()),options=option)
    #driver = webdriver.Chrome()
    #driver.set_window_size(window_size_x, window_size_y)

    res = []
    val = []
    blocked = []
    st.write('batch:',batch)
    for i in granular:
        try:
            driver.get('https://thinkhazard.org/en/')
            #assert "Python" in driver.title
            driver.find_element(By.XPATH,'//*[@id="myModal"]/div/div/div[2]/button[2]').click()
            driver.find_element(By.XPATH,'/html/body/div[2]/div/form/span[2]/input[2]').send_keys(i)
            driver.implicitly_wait(60)
            driver.find_element(By.XPATH, '//*[@id="search"]/span[2]/div/div/div[1]').click()
            driver.implicitly_wait(60)
            URL = driver.current_url
            resp = requests.get(URL)
            soup = BeautifulSoup(resp.text, features='lxml')

            for j in soup.find_all('h2', {'class': 'page-header'}):
                val.append(i)
                res.append(j.get_text())

        except:
            print(i, 'blocked')
            blocked.append(i)

        #pbar.update(1)
        start += 1

    processed = [clean_and_split(value) for value in res]

    df = pd.DataFrame(processed, columns=['Col', 'level'])
    df['Country'] = data_cop['Country']
    df['Region HL'] = data_cop['Region (HL)']
    df['Region (Granular)'] = val
    block = pd.DataFrame(blocked, columns=['blocked'])


    table = pd.pivot_table(df, index=['Region (Granular)'], columns='Col', values='level', aggfunc=lambda x: ' '.join(x), sort = False)
    #table['Region (Granular)'] = table['Region (Granular)'].astype(str)
    #final = pd.merge(table, data_to_merge, on =
    #final = pd.concat([table, data_to_merge.set_index('Region (Granular)')], axis=1).reset_index()
    final = pd.merge(data_to_merge, table, on='Region (Granular)')
    final = final[['Country', 'Region (HL)', 'Region (Granular)', 'River flood', 'Coastal flood', 'Wildfire', 'Urban flood', 'Landslide', 'Tsunami', 'Water scarcity', 'Extreme heat', 'Cyclone', 'Volcano', 'Earthquake']]
    
    output_data = output_data._append(final)
    blocked_data = blocked_data._append(block)

    if batch <= total_batches:
        batch += 1 
        
    if end < len(dataset):
        end += batch_size 
    
st.write("loop ended")
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

csv = convert_df(output_data)
csv_blocked = convert_df(blocked_data)

st.download_button(
   "Press to Download output",
   csv,
   "file.csv",
   "text/csv",
   key='download-csv'
)

#SAVE
#print(output_data)
#output_data.to_excel(r"\Users\anushk.farkiya\PycharmProjects\scraper\final_output_2.xlsx", index = True)
#blocked_data.to_excel(r'\Users\anushk.farkiya\PycharmProjects\scraper\blocked_batch_2.xlsx')

#NOTES
#please delete column A in the Excel file
