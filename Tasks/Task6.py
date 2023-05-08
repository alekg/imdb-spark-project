from pyspark import SparkConf
from pyspark.sql import SparkSession, Window

import pyspark.sql.functions as f
from Utills import FileUtills
import filelist
import schemas


def task6():
    print('==============  Task6 ===============\n\n')

    session = (SparkSession.builder
               .master('local')
               .appName('imdb')
               .config(conf=SparkConf())
               .getOrCreate())

    filename = FileUtills.get_datafile_from_url(filelist.TITTLE_BASICS_URL)

    movies_dataframe = session.read.csv(path=filename, schema=schemas.schema_tittle_basics, header=True, sep='\t',
                                        nullValue='null',
                                        dateFormat='YYYY')
    movies_dataframe = (movies_dataframe.filter(f.col('titleType') == 'tvSeries')
                        .select(f.col('tconst'), f.col('titleType'), f.col('primaryTitle')))

    filename = FileUtills.get_datafile_from_url(filelist.TITTLE_EPISODES_URL)
    episodes_dataframe = session.read.csv(path=filename, schema=schemas.schema_tittle_episodes, header=True, sep='\t',
                                          nullValue='null')

    episodes_dataframe = (episodes_dataframe
                          .na.fill(value=0)
                          .select(f.col('parentTconst').alias('tconst'), f.col('episodeNumber')))


    movies_dataframe = (movies_dataframe.join(episodes_dataframe, on='tconst', how='inner')
                        .groupBy(f.col('tconst'), f.col('primaryTitle'))
                        .sum('episodeNumber')
                        .withColumnRenamed('sum(episodeNumber)', 'episodeNumbers')
                        .orderBy('episodeNumbers', ascending=False)
                        .limit(50))


    FileUtills.save_df_to_file(movies_dataframe, 'Result6.csv')
    print('\n\n==============  End Task6 ===============\n\n')
