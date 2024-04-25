import pandas as pd
import numpy as np
import nltk
from transformers import pipeline
import os
import glob
import tqdm
import re
import subprocess
from bs4 import BeautifulSoup
import re
import urllib

def get_url_text(raw_html):
    all_data = []
        # Turning all data to dictionary with url as a key and whole info as htlm meta data
    for e in raw_html:
        # mini_dic = {}
        if '!DOCTYPE html' in e:
            url = e.split('\n')[2]
            # mini_dic[url] =  e
            all_data.append({url:e})
    return all_data

def get_body_div(dictionary_of_url_text):
    get_the_content = []
    for i in tqdm.tqdm(dictionary_of_url_text):
        for k, v in i.items():
            # if 'lang="ru"' in v or 'lang="kk"' in v:
            if 'lang="kk"' in v or 'lang="kaz"' in v:
                soup = BeautifulSoup(v)
                if 'inform.kz' in k:
                    element = soup.find('div', {'class':'article__body-content'})
                    if element != None:
                        get_the_content.append({k:element})
                elif 'nur.kz' in k:
                    element = soup.find('div', {'class':'formatted-body__content--wrapper'})
                    if element != None:
                        get_the_content.append({k:element})
    return get_the_content

if __name__ == '__main__':
    print('All functions for cleaning the data')