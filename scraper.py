#Step 1: Import Libraries
import sys
import streamlit as st
import pandas as pd
import numpy as np
import requests
import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import time
import os
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

#UI - file upload
st.title('Project Crawler!')

with st.form("my-form", clear_on_submit=True):
    uploaded_file = st.file_uploader("upload file")
    submitted = st.form_submit_button("submit")

##Step 2: Import Data and data transformation
dataframe = pd.read_excel(uploaded_file)
dataset = dataframe[:10000]
st.write(dataset)

#Change this values accordingly
window_size_x = 800 #Enable only when to see the crawler
window_size_y = 800
batch_size = 200
#ChromeDrive_path = r"D:\Users\anushk.farkiya\Downloads\chromedriver_win32"

#Initialize DF
output_data = pd.DataFrame()
blocked_data = pd.DataFrame()
total_iterations = len(dataset)

#Define clean and dataframe functions
@st.cache_data
def clean_and_split(value):
    cleaned = value.strip()
    split = cleaned.split("\n")
    return [split.strip() for split in split if split.strip()]

@st.cache_data
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

#Initialize values 
start = 0
end = batch_size
batch = 1
total_batches = int(len(dataset)/batch_size)
st.write('total batches', total_batches) 

columns = ['Region (Granular)', 'River flood', 'Coastal flood', 'Wildfire', 'Urban flood', 'Landslide', 'Tsunami', 'Water scarcity', 'Extreme heat', 'Cyclone', 'Volcano', 'Earthquake']
#Initialize CSV file
with open('final.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(columns)


#MAIN WHILE LOOP
while start < end:
    data_cop = dataset[start:end]
    #data_to_merge = data_cop[['Country','Region (HL)','Region (Granular)']]
    #data_to_merge['Region (Granular'] = data_to_merge['Region (Granular)'].astype(str)
    granular = data_cop['Region (Granular)']

    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless")

    service = Service()
    driver = webdriver.Chrome(service=service, options=options)
    #driver.set_window_size(window_size_x, window_size_y)

    res = []
    val = []
    blocked = []
    st.write('batch:',batch)
    for i in granular:
        try:
            driver.get('https://thinkhazard.org/en/')
            driver.find_element(By.XPATH,'//*[@id="myModal"]/div/div/div[2]/button[2]').click()
            driver.find_element(By.XPATH,'/html/body/div[2]/div/form/span[2]/input[2]').send_keys(i)
            driver.implicitly_wait(5)
            driver.find_element(By.XPATH, '//*[@id="search"]/span[2]/div/div/div[1]').click()
            driver.implicitly_wait(10)
            URL = driver.current_url
            resp = requests.get(URL)
            soup = BeautifulSoup(resp.text, features='lxml')

            for j in soup.find_all('h2', {'class': 'page-header'}):
                val.append(i)
                res.append(j.get_text())

        except:
            print(i, 'blocked')
            blocked.append(i)

        start += 1
        #st.write("start", start)
        

    processed = [clean_and_split(value) for value in res]

    df = pd.DataFrame(processed, columns=['Col', 'level'])
    
    df['Region (Granular)'] = val
    block = pd.DataFrame(blocked, columns=['blocked'])


    table = pd.pivot_table(df, index=['Region (Granular)'], columns='Col', values='level', aggfunc=lambda x: ' '.join(x), sort = False)
    table = table.reset_index()
    st.write(table)
    table = table[['Region (Granular)', 'River flood', 'Coastal flood', 'Wildfire', 'Urban flood', 'Landslide', 'Tsunami', 'Water scarcity', 'Extreme heat', 'Cyclone', 'Volcano', 'Earthquake']]
    
    #final = pd.concat([table, data_to_merge.set_index('Region (Granular)')], axis=1).reset_index()
    #final = pd.merge(data_to_merge, table, on='Region (Granular)')
    #final = final[['Country', 'Region (HL)', 'Region (Granular)', 'River flood', 'Coastal flood', 'Wildfire', 'Urban flood', 'Landslide', 'Tsunami', 'Water scarcity', 'Extreme heat', 'Cyclone', 'Volcano', 'Earthquake']]
    
    # final.to_excel(writer, sheet_name =f'{batch}', index=False) 

    #output_data = output_data._append(table)
    blocked_data = blocked_data._append(block)

    with open('final.csv', mode='a', newline='') as file:
        csv_writer = csv.writer(file)
        for _, row in table.iterrows():
            csv_writer.writerow(row)

    if batch <= total_batches:
        batch += 1 
        
    if end < len(dataset):
        end += batch_size 
    
    st.write("end", end)
    st.write("start", start)

#csv = convert_df(output_data)

st.write(blocked_data)

st.download_button(
    "Press to Download output",
    open('final.csv', 'rb').read(),
    "final.csv",
    "text/csv",
    key='download-csv'
)
