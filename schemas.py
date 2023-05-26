import pyspark.sql.types as t

schema_tittle_akas = t.StructType([
    t.StructField('titleId', t.StringType(), True),
    t.StructField('ordering', t.IntegerType(), True),
    t.StructField('title', t.StringType(), True),
    t.StructField('region', t.StringType(), True),
    t.StructField('language', t.StringType(), True),
    t.StructField('types', t.StringType(), True),
    t.StructField('attributes', t.StringType(), True),
    t.StructField('isOriginalTitle', t.BooleanType(), True)])

schema_name_basics = t.StructType([
    t.StructField('nconst', t.StringType(), True),
    t.StructField('primaryName', t.StringType(), True),
    t.StructField('birthYear', t.IntegerType(), True),
    t.StructField('deathYear', t.IntegerType(), True),
    t.StructField('primaryProfession', t.StringType(), True),
    t.StructField('knownForTitles', t.StringType(), True)])

schema_tittle_basics = t.StructType([
        t.StructField('tconst', t.StringType(), True),
        t.StructField('titleType', t.StringType(), True),
        t.StructField('primaryTitle', t.StringType(), True),
        t.StructField('originalTitle', t.StringType(), True),
        t.StructField('isAdult', t.IntegerType(), True),
        t.StructField('startYear', t.StringType(), True),
        t.StructField('endYear', t.StringType(), True),
        t.StructField('runtimeMinutes', t.IntegerType(), True),
        t.StructField('genres', t.StringType(), True)])


schema_tittle_participans = t.StructType([
        t.StructField('tconst', t.StringType(), True),
        t.StructField('ordering', t.IntegerType(), True),
        t.StructField('nconst', t.StringType(), True),
        t.StructField('category', t.StringType(), True),
        t.StructField('job', t.StringType(), True),
        t.StructField('characters', t.StringType(), True)])

schema_tittle_episodes = t.StructType([
        t.StructField('tconst', t.StringType(), True),
        t.StructField('parentTconst', t.StringType(), True),
        t.StructField('seasonNumber', t.IntegerType(), True),
        t.StructField('episodeNumber', t.IntegerType(), True)])
schema_ratings=t.StructType([
        t.StructField('tconst', t.StringType(), True),
        t.StructField('averageRating', t.FloatType(), True),
        t.StructField('numVotes', t.IntegerType(), True)])
