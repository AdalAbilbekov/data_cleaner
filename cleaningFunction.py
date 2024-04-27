import pandas as pd
import numpy as np
import nltk
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
import time
from datetime import datetime
import os
from pathlib import Path
from lingua import Language, LanguageDetectorBuilder

# /nutch/segments 23 52 simultaneously_segment_clean

def cleaning_dump_extract_csv(path_to_nutch, path_to_csv, path_to_java_home, path_to_dumps):
    print('The starting process...')

    list_of_segments = sorted(Path('/home/batyr/workspace/apache-nutch-1.19/correct_urls/segments').iterdir(), key=os.path.getmtime)
    list_of_segments = [str(dir).split('/')[-1] for dir in list_of_segments] 

    if len(list_of_segments) > 1:
        list_of_segments = os.listdir(f'{path_to_nutch}/correct_urls/segments')[:-1]
    else:
        list_of_segments = 'no segments'
    
    csv_files = [i[:-4] for i in os.listdir(path_to_csv)]
    print(f'Number of segments are {len(list_of_segments)}')

    segments_to_process = set(list_of_segments) - set(csv_files)
    print(f'Number of segments process are {len(segments_to_process)}')

    os.environ['JAVA_HOME'] = path_to_java_home

    # Creating all the dumps at a time is storage consuming.
    # for segment in tqdm.tqdm(segments_to_process):
    #     if segment not in csv_files:
    #         print(f'Creating segment {segment}')
    #         subprocess.run(f'{path_to_nutch}/bin/nutch readseg -dump {path_to_nutch}/nutch/segments/{segment} {path_to_nutch}/{path_to_dumps}/{segment}', shell=True)
    #         print('___________________________________________________________________________________________')
    # print('Getting the dumps out of segments file ...')

    # dumps = os.listdir(f'{path_to_nutch}/nutch_core_segment_dumps')
    # theExistingData = os.listdir('/mnt/storage/crowl/adal_work/the_whole_cleaned_data')

    # print(f'The dumps: {dumps}')
    
    # the_total_cleaned_data = []
    
    if len(segments_to_process) > 0 and list_of_segments != 'no segments':
        start = datetime.now()
        languages = [Language.ENGLISH, Language.ARABIC, Language.ARMENIAN, Language.AZERBAIJANI, Language.CHINESE, Language.KAZAKH, Language.MONGOLIAN, Language.RUSSIAN, Language.TURKISH]
        detector = LanguageDetectorBuilder.from_languages(*languages).build()
        for segment in tqdm.tqdm(segments_to_process):
            print('Starting the dump extraction process ...')
            if segment not in csv_files:
                print(f'Creating dump: {segment}')
                subprocess.run(f'{path_to_nutch}/bin/nutch readseg -dump {path_to_nutch}/correct_urls/segments/{segment} {path_to_nutch}/{path_to_dumps}/{segment}', shell=True)

            dump = segment

            with open(f'{path_to_nutch}/{path_to_dumps}/{dump}/dump', 'r') as dump_file:
                all_content = dump_file.read()

            # splitting the data by Content::
            print('Starting the cleaning process ...')
            recno = all_content.split('Recno:')

            uzbek_l1, uzbek_l2, uzbek_l1U, uzbek_l2U = 'ў', 'ҳ', 'ў'.upper(), 'ҳ'.upper()
            the_data = []
            for mini_recno in tqdm.tqdm(recno):
                if 'Version: -1' in mini_recno:
                    url = mini_recno.split('Version: -1')[1].split('Content:')[0].split('\n')[1]
                    raw_html = f"{mini_recno.split('Version: -1')[1].split('Content:')[1].split('</html>')[0]}</html>"
                    # if 'lang="ru"' in raw_html or 'lang="kk"' in raw_html:
                    text = trafilatura.extract(raw_html)
                    if text != None:
                        if uzbek_l1 in text or uzbek_l2 in text or uzbek_l1U in text or uzbek_l2U in text:
                            continue
                        else:
                            lst_text = text.split('\n')
                            joint_text = []
                            # print(text)
                            for sentence in lst_text:
                                if len(sentence) > 0 and len(sentence.split()) >= 5:
                                    joint_text.append(sentence)
                            joint_text = list(pd.Series(joint_text).drop_duplicates())
                            cleaned_text = '\n'.join(joint_text)
                            if len(cleaned_text) > 1 and len(cleaned_text.replace('\n', ' ').split()) > 30:
                                the_data.append({url:cleaned_text})

            # the_total_cleaned_data.append(all_new_lvl)
            all_data = []

            for dictionary in the_data:
                mua = [{"url":key.replace('url: ', ''), "data": value} for key, value in dictionary.items()]
                all_data += mua           

            df = pd.DataFrame(all_data)

            if df.shape[0] > 0:
                language_detection = [str(detector.detect_language_of(paragraph)).replace('Language.', '') for paragraph in df['data']]
                df['language_id'] = language_detection
                needed_languages = ['ENGLISH', 'KAZAKH', 'RUSSIAN']
                df = df[df['language_id'].isin(needed_languages)]

                df.to_csv(f'{path_to_csv}/{dump}.csv', index=False)

            if dump in os.listdir(f'{path_to_nutch}/{path_to_dumps}'):
                print(f'The dump {dump} is deleted ...')
                subprocess.run(f'rm -rf {path_to_nutch}/{path_to_dumps}/{dump}', shell=True)
        
        end = datetime.now()
        td = (end - start).total_seconds() / 60
        print(f"The time of execution of above program is : {td:.03f}min")
    else:
        print('There are no new segments')

if __name__ == "__main__":
    args = get_arguments_for_cleaning()

    nutrch_d = args.nutch_path
    java_home = args.java_env
    dumps_d = args.dumps
    csv_d = args.csv
    iteration = 0
    size = os.get_terminal_size() 
    while True:
        iteration += 1
        print(f'New iteration has started, The iteration: {iteration}')
        cleaning_dump_extract_csv(nutrch_d, csv_d, java_home, dumps_d)
        print("-"*size.columns)
        time.sleep(120)
