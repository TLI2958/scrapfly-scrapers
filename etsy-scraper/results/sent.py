from transformers import AutoTokenizer
from transformers import AutoModelForSequenceClassification
from datasets import Dataset, load_dataset
import torch
import torch.nn as nn
from torch.nn import functional as F
from torch.utils.data import DataLoader
from tqdm import tqdm
import evaluate
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os 
import text_cleaning
import random
import pandas as pd
from collections import Counter
import pickle
import joblib
import csv
import json

import warnings
import sklearn.exceptions
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings("ignore", category=sklearn.exceptions.UndefinedMetricWarning)

violet = '#702b9d'
RANDOM_SEED = 3001
batch_size = 8
device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

def tokenize_function(examples):
    tok = tokenizer(examples["text"], padding=True, truncation=True, 
                    return_tensors="pt")
    examples['text'] = tok['input_ids']
    return examples 


def do_infer(infer_data, output_dir):

    model = AutoModelForSequenceClassification.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment")     
    model.to(device)
    model.eval()
    
    headers = [
    'asin', 'brand', 'name', 'description', 'features', 'stars', 
    'rating_count', 'review_title', 'review_rating', 
    'review_location_and_date', 'verified', 'review_body', 'images', 'sentiment']
    
    output_file = output_dir + '/preds.csv'
    print(output_file)
    
    with torch.no_grad():
        with open(output_file, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                for i in tqdm(range(0, len(infer_data), batch_size)):
                    batch = infer_data[i:i + batch_size]
                    input = torch.tensor(batch["text"]).to(device)
                    
                    outputs = model(input)
                    logits = outputs.logits
                    predictions = torch.argmax(logits, dim=-1)
                    
                    for j in range(predictions.shape[0]):
                        writer.writerow(batch[j] + ['negative', 'neutral', 'positive'][predictions[j].item()])

        output_file.close() 
    print('inference completed ...')
    return 


if __name__ == '__main__':
    os.chdir('$SCRATCH/scrapfly-scrapers/amazon-scraper/') 
    tokenizer = AutoTokenizer.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment")
    ## preprocess, tokenize, loader
    amazon_reviews = Dataset.from_csv('results/Amazon_reviews_w_sentiment.csv')

    amazon_reviews_ = amazon_reviews.map(lambda x: text_cleaning.text_cleaning(x, map_label = False))
    amazon_reviews_ = amazon_reviews_.map(tokenize_function, batched = True, load_from_cache_file = False)
    amazon_reviews_.set_format('torch')

    do_infer(amazon_reviews_, os.getcwd())