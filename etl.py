import requests
import pandas as pd
from sqlalchemy import create_engine

def extract() -> dict:
    url = 'http://universities.hipolabs.com/search?country=United+States'
    response = requests.get(url)
    return response.json()

def transform(data) -> pd.DataFrame:
    df = pd.DataFrame(data)
    print("The number of universities in the US is: ", df.shape[0])
    df = df[df['name'].str.contains('California')]
    print("The number of universities in California is: ", df.shape[0])
    df["domains"] = [','.join(map(str, l)) for l in df["domains"]]
    df["web_pages"] = [','.join(map(str, l)) for l in df["web_pages"]]
    df = df.reset_index(drop=True)
    return df[["name", "domains", "web_pages", "country"]]

def load(data) -> None:
    engine = create_engine('sqlite:///my_lite_store.db')  
    data.to_sql('universities', con=engine, if_exists='replace', index=False)

def etl_pipeline() -> None:
    data = extract()
    data = transform(data)
    load(data)

if __name__ == '__main__':
    etl_pipeline()
