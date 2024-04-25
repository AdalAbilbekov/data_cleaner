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
from utils import get_arguments_for_cleaning

if __name__ == "__main__":
    
    args = get_arguments_for_cleaning()

    nutrch_d = args.nutch_path
    java_home = args.java_env
    dumps_d = args.dumps
    csv_d = args.csv

    list_of_segments = os.listdir(f'{nutrch_d}/nutch/segments')

    os.environ['JAVA_HOME'] = java_home

    for segment in list_of_segments[:10]:
        subprocess.run(f'{nutrch_d}/bin/nutch readseg -dump {nutrch_d}/nutch/segments/{segment} {nutrch_d}/{dumps_d}/{segment}', shell=True)
    print('Getting the dumps out of segments file ...')

    dumps = os.listdir(f'{nutrch_d}/nutch_core_segment_dumps')
    theExistingData = os.listdir('/mnt/storage/crowl/adal_work/the_whole_cleaned_data')

    print(f'The dumps: {dumps}')
    
    the_total_cleaned_data = []
    print('Starting the cleaning process ...')
    for dump in tqdm.tqdm(dumps):
        # getting all data from a dump to one string
        with open(f'{nutrch_d}/nutch_core_segment_dumps/{dump}/dump', 'r') as dump_file:
            all_content = dump_file.read()

        # splitting the data by Content::
        doctypes = all_content.split('Content::')

        all_data = []
        # Turning all data to dictionary with url as a key and whole info as htlm meta data
        for e in doctypes:
            # mini_dic = {}
            if '!DOCTYPE html' in e:
                url = e.split('\n')[2]
                # mini_dic[url] =  e
                all_data.append({url:e})
                
        get_the_content = []
        for i in tqdm.tqdm(all_data):
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

        all_new_lvl = []
        for i in tqdm.tqdm(get_the_content):
            new_p = []
            for k, v in i.items():
                all_parapgraphs = v.find_all('p')
                for par in all_parapgraphs:
                    # print(par)
                    new_p.append(par.get_text())
                fully_cleaned = '\n'.join(new_p)
                all_new_lvl.append({k:fully_cleaned})
        print(f'Current dumps data : {len(all_new_lvl)}')
        # the_total_cleaned_data.append(all_new_lvl)

        s = []
    # for list_of_data in tqdm.tqdm(the_total_cleaned_data):
        for dictionary in all_new_lvl:
            mua = [{"url":key.replace('url: ', ''), "data": value} for key, value in dictionary.items()]
            s += mua
        print(f'The Dump {dump} is saved.')
        df = pd.DataFrame(s)
        df.to_csv(f'{csv_d}/{dump}.csv', index=False)
        if dump in os.listdir(f'{nutrch_d}/nutch_core_segment_dumps'):
            print(f'The dump {dump} is deleted ...')
            subprocess.run(f'rm -rf {nutrch_d}/nutch_core_segment_dumps/{dump}', shell=True)