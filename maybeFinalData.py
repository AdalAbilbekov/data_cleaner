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
import cleaning_data as cl
import trafilatura

if __name__ == "__main__":
    print('The starting process...')
    args = get_arguments_for_cleaning()

    nutrch_d = args.nutch_path
    java_home = args.java_env
    dumps_d = args.dumps
    csv_d = args.csv

    list_of_segments = os.listdir(f'{nutrch_d}/nutch/segments')
    csv_files = [i[:-4] for i in os.listdir(csv_d)]

    os.environ['JAVA_HOME'] = java_home


    for segment in tqdm.tqdm(list_of_segments[0:4]):
        if segment not in csv_files:
            print('Creating segment {segment}')
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
        recno = all_content.split('Recno:')

        the_data = []
        counter_before = 0
        for mini_recno in tqdm.tqdm(recno):
            if 'Version: -1' in mini_recno:
                url = mini_recno.split('Version: -1')[1].split('Content:')[0].split('\n')[1]
                raw_html = f"{mini_recno.split('Version: -1')[1].split('Content:')[1].split('</html>')[0]}</html>"
                if 'lang="ru"' in raw_html or 'lang="kk"' in raw_html:
                    text = trafilatura.extract(raw_html)
                    if text != None:
                        lst_text = text.split('\n')
                        joint_text = []
                        # print(text)
                        for sentence in lst_text:
                            if len(sentence) > 0 and len(sentence.split()) > 6:
                                joint_text.append(sentence)
                        cleaned_text = '\n'.join(joint_text)
                        if len(cleaned_text) > 1:
                            the_data.append({url:cleaned_text})

        # the_total_cleaned_data.append(all_new_lvl)
        all_data = []

        for dictionary in the_data:
            mua = [{"url":key.replace('url: ', ''), "data": value} for key, value in dictionary.items()]
            all_data += mua           

        df = pd.DataFrame(all_data)
        df.to_csv(f'{csv_d}/{dump}.csv', index=False)
        if dump in os.listdir(f'{nutrch_d}/nutch_core_segment_dumps'):
            print(f'The dump {dump} is deleted ...')
            subprocess.run(f'rm -rf {nutrch_d}/nutch_core_segment_dumps/{dump}', shell=True)