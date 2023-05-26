from pyspark import SparkConf
from pyspark.sql import SparkSession, Window

import pyspark.sql.functions as f
from Utills import FileUtills
import filelist
import schemas

def task5():
    print('==============  Task5 ===============\n\n')

    session = (SparkSession.builder
               .master('local')
               .appName('imdb')
               .config(conf=SparkConf())
               .getOrCreate())


    filename = FileUtills.get_datafile_from_url(filelist.TITTLE_AKAS_URL)
    title_dataframe = session.read.csv(path=filename, schema=schemas.schema_tittle_akas, header=True, sep='\t', nullValue='null')
    title_dataframe = (title_dataframe.select(f.col('titleId').alias('tconst'), f.col('region'))
                       .filter(f.col('region') != '\\N'))


    filename = FileUtills.get_datafile_from_url(filelist.TITTLE_BASICS_URL)

    movies_dataframe = session.read.csv(path=filename, schema=schemas.schema_tittle_basics, header=True, sep='\t', nullValue='null',
                                        dateFormat='YYYY')
    movies_dataframe = (movies_dataframe.filter(f.col('isAdult') == 1)
                        .select(f.col('tconst'), f.col('isAdult')))
    title_dataframe = (title_dataframe.join(movies_dataframe, on='tconst', how='inner')
                       .groupBy(f.col('region')).count()
                       .orderBy('count', ascending=False)
                       .limit(100))


    FileUtills.save_df_to_file(title_dataframe, 'Result5.csv')
    print('\n\n==============  End Task5 ===============\n\n')
