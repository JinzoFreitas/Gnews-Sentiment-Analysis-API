import json
import urllib.request
import pandas as pd
import datetime

apikey = 'f08f7cd5b6241587e0563754d8d39ea8'

def get_news(category = 'general', lang = 'en', country = 'us', length = 10, apikey=''):
  url = f"https://gnews.io/api/v4/top-headlines?category={category}&lang={lang}&country={country}&max={length}&apikey={apikey}"
  response = urllib.request.urlopen(url)
  data = json.loads(response.read().decode("utf-8"))
  articles = data["articles"]
  return articles

def to_dataframe(articles):
  df = pd.json_normalize(articles)
  df = df[['title', 'url', 'publishedAt', 'source.name']]
  df.rename(columns = {'source.name': 'source'}, inplace = True)
  df['publishedAt'] =  pd.to_datetime(df['publishedAt'] , infer_datetime_format=True)
  return df

def save_articles(df):
  year = datetime.date.today().year     
  month = datetime.date.today().month    
  day = datetime.date.today().day      
  hour = datetime.datetime.now().hour 
  df.to_parquet(f"articles/gnews_articles_{year}_{month}_{day}_{hour}.parquet")   


articles = get_news(apikey=apikey)
df = to_dataframe(articles)
save_articles(df)