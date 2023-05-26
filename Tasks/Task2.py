from pyspark import SparkConf
from pyspark.sql import SparkSession

import pyspark.sql.functions as f
from Utills import FileUtills
import filelist
import schemas

def task2():
    print('==============  Task2 ===============\n\n')

    session = (SparkSession.builder
               .master('local')
               .appName('imdb')
               .config(conf=SparkConf())
               .getOrCreate())


    filename = FileUtills.get_datafile_from_url(filelist.NAME_BASICS_URL)
    movies_dataframe = session.read.csv(path=filename, schema=schemas.schema_name_basics, header=True, sep='\t', nullValue='null', dateFormat='YYYY')
    movies_dataframe = (movies_dataframe.select(f.col('primaryName'), f.col('birthYear'))
                        .filter(f.col('birthYear').between(1800,1899)))

    FileUtills.save_df_to_file(movies_dataframe, 'Result2.csv')
    print('\n\n==============  End Task2 ===============\n\n')