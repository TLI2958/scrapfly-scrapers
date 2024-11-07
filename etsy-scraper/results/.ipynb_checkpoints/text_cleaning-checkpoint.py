from bs4 import BeautifulSoup
from collections import defaultdict
import pandas as pd
import numpy as np
import os
import re
from unidecode import unidecode
violet = '#702b9d'


def text_cleaning(example, map_label = True):
    '''
    Cleans text into a basic form for NLP. Operations include the following:-
    1. Remove special charecters like &, #, etc
    2. Removes extra spaces
    3. Removes embedded URL links
    4. Removes HTML tags
    5. Removes emojis -> no need for this project
    
    text - Text piece to be cleaned.

    adapted from https://www.kaggle.com/code/manabendrarout/pytorch-roberta-ranking-baseline-jrstc-train
    new editions as of Apr 24' 
    ''' 
    template = re.compile(r'https?://\S+|www\.\S+') # Removes website links
    text = template.sub(r'', example['review_body'])
    
    soup = BeautifulSoup(text, 'lxml') #Removes HTML tags
    only_text = soup.get_text()
    text = only_text
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text)
    
    text = re.sub(r"[^a-zA-Z\d]", " ", text) # Remove special Charecters
    text = re.sub(' +', ' ', text) # Remove Extra Spaces
    text = text.strip() # remove spaces at the beginning and at the end of string
    example['text'] = text

    if map_label:
        label = example['label']
        label = 2 if x == 1 else 1 if x == 2 else 0
        example['label'] = label
    return example


def rel_map(df):
    df = pd.read_csv(df, index_col= 0)
    # df.reset_index(inplace = True)
    # df.drop(columns = 'index', inplace = True)
    df = df[df.Relevance.apply(lambda x: type(x) in [int, float] or str(x).isdigit())]

    df.Relevance = df.Relevance.apply(lambda x: int(x))
    df_rel = df.groupby(['Vocab'])['Relevance'].mean()
    
    df_map = dict(zip(df_rel.index, np.arange(len(df_rel))))
    return df_rel.to_numpy(), df_map
    

def s2i(example, mapping):
    to_map = example['text'].lower().split()
    if len(to_map) > 1:
        to_map += [to_map[0] + ' ' + to_map[1]]  
    mapped = [mapping.get(m, -1) for m in to_map]  
    example['text'] = mapped 
    return example

def sent_per_date(df, ind, name = 'apple'):
    df.iloc[~df.index.isin(ind), 1] = 0
    
    sent_per_date = df.groupby('date').mean()
    sent_per_date.to_csv(f'output/{name}_sent.csv')
    sent_per_date.hist(color = violet, alpha = .7)

def train_val_test_split(df, train_end, val_end):
    assert 'tic' in df.columns
    assert 'date' in df.columns
    df_train, df_test = df.loc[df.date < val_end], df.loc[df.date >= val_end]
    df_train, df_val = df_train.loc[df_train.date < train_end], df_train.loc[df_train.date >= train_end]
    df_train.index = df_train.groupby('tic').cumcount()
    df_val.index = df_val.groupby('tic').cumcount()
    df_test.index = df_test.groupby('tic').cumcount()
    return df_train, df_val, df_test