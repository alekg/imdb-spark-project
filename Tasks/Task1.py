from pyspark import SparkConf
from pyspark.sql import SparkSession

import pyspark.sql.functions as f
from Utills import FileUtills
import filelist
import schemas


def task1():
    print('==============  Task1 ===============\n\n')

    session = (SparkSession.builder
               .master('local')
               .appName('imdb')
               .config(conf=SparkConf())
               .getOrCreate())


    filename = FileUtills.get_datafile_from_url(filelist.TITTLE_AKAS_URL)
    movies_dataframe = session.read.csv(path=filename, schema=schemas.schema_tittle_akas, header=True, sep='\t', nullValue='null')
    movies_dataframe = movies_dataframe.filter(f.col('region') == 'UA').select(f.col('title'), f.col('region'))


    FileUtills.save_df_to_file(movies_dataframe, 'Result1.csv')
    print('\n\n==============  End Task1 ===============\n\n')
