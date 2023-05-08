from pyspark import SparkConf
from pyspark.sql import SparkSession, Window

import pyspark.sql.functions as f
from Utills import FileUtills
import filelist
import schemas


def task8():
    print('==============  Task8 ===============\n\n')

    session = (SparkSession.builder
               .master('local')
               .appName('imdb')
               .config(conf=SparkConf())
               .getOrCreate())

    filename = FileUtills.get_datafile_from_url(filelist.TITTLE_RATINGS_URL)

    ratings_dataframe = session.read.csv(path=filename, schema=schemas.schema_ratings, header=True, sep='\t',
                                        nullValue='null')

    ratings_dataframe = (ratings_dataframe
                        .select(f.col('tconst'), f.col('averageRating')))

    filename = FileUtills.get_datafile_from_url(filelist.TITTLE_BASICS_URL)

    movies_dataframe = session.read.csv(path=filename, schema=schemas.schema_tittle_basics, header=True, sep='\t',
                                        nullValue='null',
                                        dateFormat='YYYY')


    movies_dataframe = (movies_dataframe
                        .filter( f.col('genres') != '\\N')
                        .select(f.col('tconst'), f.col('primaryTitle'), f.explode(f.split(f.col('genres'), ',').alias('genre')).alias('genre'))
                        .drop('genres'))


    windowDept = Window.partitionBy('genre').orderBy(f.col('averageRating').desc())
    movies_dataframe = (movies_dataframe.join(ratings_dataframe, on='tconst', how='inner')
                        .withColumn('cnt',f.row_number().over(windowDept))
                        .filter(f.col('cnt') <= 10)
                        .select(f.col('genre'),f.col('cnt'), f.col('primaryTitle') , f.col('averageRating')))



    FileUtills.save_df_to_file(movies_dataframe, 'Result8.csv')
    print('\n\n==============  End Task8 ===============\n\n')
