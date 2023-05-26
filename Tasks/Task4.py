from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from Utills import FileUtills
import filelist
import schemas


def task4():
    print('==============  Task4 ===============\n\n')

    session = (SparkSession.builder
               .master('local')
               .appName('imdb')
               .config(conf=SparkConf())
               .getOrCreate())


    filename_p = FileUtills.get_datafile_from_url(filelist.TITTLE_PRINCIPALS_URL)
    person_dataframe = session.read.csv(path=filename_p, schema=schemas.schema_tittle_participans, header=True, sep='\t', nullValue='null')
    person_dataframe = (person_dataframe.drop(f.col('job'), f.col('ordering'))
                        .filter(f.col('category').isin('actress', 'actor') & (f.col('characters') != '\\N')))


    filename = FileUtills.get_datafile_from_url(filelist.TITTLE_BASICS_URL)
    movies_dataframe = session.read.csv(path=filename, schema=schemas.schema_tittle_basics, header=True, sep='\t', nullValue='null',
                                        dateFormat='YYYY')
    movies_dataframe = (movies_dataframe.filter(f.col('titleType').isin('movie', 'tvSeries', 'tvMovie'))
                        .select(f.col('tconst'), f.col('titleType'), f.col('primaryTitle'), f.col('originalTitle')))


    filename = FileUtills.get_datafile_from_url(filelist.NAME_BASICS_URL)
    names_dataframe = session.read.csv(path=filename, schema=schemas.schema_name_basics, header=True, sep='\t', nullValue='null',
                                        dateFormat='YYYY')
    names_dataframe = names_dataframe.select(f.col('nconst'), f.col('primaryName'))

    person_dataframe = person_dataframe.join(names_dataframe, on='nconst' , how='left')
    movies_dataframe = movies_dataframe.join(person_dataframe, on='tconst', how='inner').drop(f.col('tconst'), f.col('nconst'))

    FileUtills.save_df_to_file(movies_dataframe, 'Result4.csv')
    print('\n\n==============  End Task4 ===============\n\n')
