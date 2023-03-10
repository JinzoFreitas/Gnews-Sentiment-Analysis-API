import json
import requests
import pandas as pd
import time 
import datetime


def get_analysis(text):
  url_text_classification = "https://api.meaningcloud.com/sentiment-2.1"
  payload={
    'key': '3c47617add847f1191598f1fa9707e07',
    'txt': text,
    'lang': 'en',  
  }
  response_classification = requests.post(url_text_classification, data=payload)
  agreement = response_classification.json()['agreement']
  confidence = response_classification.json()['confidence']
  irony = response_classification.json()['irony']
  score_tag = response_classification.json()['score_tag']
  df_temp = pd.DataFrame(columns=['title', 'score_tag', 'agreement', 'confidence', 'irony'])
  df_temp = df_temp.append({'title': text, 'score_tag': score_tag, 'agreement': agreement, 'confidence': confidence, 'irony': irony}, ignore_index=True)
  return df_temp

def get_analyses(df):
  df_analysis = pd.DataFrame(columns = ['title', 'score_tag', 'agreement', 'confidence', 'irony'])
  for index, row in df.iterrows():
    try:
      df_temp = get_analysis(row['title'])
      df_analysis = pd.concat([df_analysis, df_temp])
    except KeyError:
      print('Deu erro:', row['title'])
    time.sleep(5)
  return df_analysis

def generate_dataframe(df_1, df_2):
  year = datetime.date.today().year     
  month = datetime.date.today().month    
  day = datetime.date.today().day      
  hour = datetime.datetime.now().hour 
  df_final = df_1.merge(df_2, on = 'title', how= 'left')
  df_final.to_parquet(f"dw/analise_sentimento_{year}_{month}_{day}_{hour}.parquet")


year = datetime.date.today().year     
month = datetime.date.today().month    
day = datetime.date.today().day      
hour = datetime.datetime.now().hour
parquet = f'gnews_app/articles/gnews_articles_{year}_{month}_{day}_{hour}.parquet'

df_gnews = pd.read_parquet(parquet)
df_analysis = get_analyses(df_gnews)
generate_dataframe(df_gnews, df_analysis)