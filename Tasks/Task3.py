from pyspark import SparkConf
from pyspark.sql import SparkSession, Window

import pyspark.sql.functions as f
from Utills import FileUtills
import filelist
import schemas

def task3():

    print('==============  Task3 ===============\n\n')
    session = (SparkSession.builder
               .master('local')
               .appName('imdb')
               .config(conf=SparkConf())
               .getOrCreate())


    filename = FileUtills.get_datafile_from_url(filelist.TITTLE_BASICS_URL)
    movies_dataframe = session.read.csv(path=filename, schema=schemas.schema_tittle_basics, header=True, sep='\t', nullValue='null', dateFormat='YYYY')
    movies_dataframe = (movies_dataframe.filter(f.col('titleType') == 'movie')
                        .filter(f.col('runtimeMinutes') > 120))

    FileUtills.save_df_to_file(movies_dataframe, 'Result3.csv')
    print('\n\n==============  End Task3 ===============\n\n')
