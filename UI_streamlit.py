import streamlit as st
import os
import pandas as pd
from PIL import Image
import sys

# cd Option_Chain_scripts
# streamlit run UI_streamlit.py
st.set_page_config(layout="wide")

directory_of_python_script = os.path.dirname(os.path.abspath(__file__))
all_files = os.listdir(directory_of_python_script)    
csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
csv_files_name = [os.path.splitext(each)[0] for each in csv_files]
# print(csv_files_name)

image_path = os.path.join(directory_of_python_script, "nse logo.png")
image =  Image.open(image_path)

st.image(image)
st.title('Option Chain Live Data Dashboard')

def main():
    Expiry_Date = []
    scripts = st.selectbox('Select Scripts', options=['select']+csv_files_name)
    if scripts != 'select':
        df = pd.read_csv(os.path.join(directory_of_python_script, f"{scripts}.csv"), on_bad_lines='skip')
        df.columns = ['Index','TimeStamp', 'Symbol', 'UNDERLYING', 'CALL_P_CHNG', 'CALL_CHNG', 'CALL_IV', 'CALL_OI', 'CALL_CHNG_OI', 'STRIKE_PRICE', 'EXPIRY_DATE', 'PUT_P_CHNG', 'PUT_CHNG', 'PUT_IV', 'PUT_OI', 'PUT_CHNG_OI', 'PCR', 'PCR_avrg', 'resistance', 'support']
        df = df.drop('Index', axis=1)
        Expiry_Date.extend(df.iloc[:, 9].tolist())
        Expiry_Date = list(set(Expiry_Date))
        Expiry_Date = st.selectbox('Select Expiry Date', options=Expiry_Date)
    if st.button('Submit'):
        st.dataframe(df[(df['Symbol'] == scripts) & (df['EXPIRY_DATE'] == Expiry_Date) ])

main()    