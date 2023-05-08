import os
from urllib.parse import urlparse
import requests



def get_datafile_from_url(url):

    filename = os.path.basename(urlparse(url).path)
    local_path = './data/'+filename
    if not os.path.isdir('./data'):
        print('Directory (data) does not exist. Creating ...')
        os.makedirs('./data')
        print('Directory (data) created')
    if not os.path.exists(local_path):
        print(f'File {local_path} does not exist. Starting downloading ...')
        url_file = requests.get(url)
        with open(local_path, 'wb') as f:
            f.write(url_file.content)
        print(f'File {local_path} downloaded')
    return local_path




def save_df_to_file(df, filename):
    if not os.path.isdir('./results'):
        print('Directory (results) does not exist. Creating ...')
        os.makedirs('./results')
        print('Directory (results) created')
    print(f'Writing results to file ./results/{filename} ...')
    df.write.csv(path='./results/'+filename, mode='overwrite', header=True)
    print('Done successfully')
