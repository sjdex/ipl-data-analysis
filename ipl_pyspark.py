from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.appName("Transform CSV").getOrCreate()

# Read the CSV file from S3
s3_input_path = "s3://ipl-raw-data/ipl_raw.csv"
df = spark.read.csv(s3_input_path, header=True, inferSchema=True)

# Perform transformations
venue_df = df.select(col('id').alias('match_id'), 'venue', 'city')

match_df = df.select(col('id').alias('match_id'), 'date', 'team1', 'team2', 'toss_winner', col('winner').alias('match_winner'), 'player_of_match', 'umpire1', 'umpire2')

match_df = match_df.withColumn('match_loser', when(col('match_winner') == col('team1'), col('team2')).when(col('match_winner') == col('team2'), col('team1')).otherwise(None))

matches = match_df.groupBy('team1').count() \
                  .union(match_df.groupBy('team2').count()) \
                  .groupBy('team1').sum("count").withColumnRenamed("sum(count)", "total_matches")

match_winner_df = match_df.groupBy('match_winner').count().withColumnRenamed("count", "wins")
match_loser_df = match_df.groupBy('match_loser').count().withColumnRenamed("count", "losses")

team_df1 = matches.join(match_winner_df, matches.team1 == match_winner_df.match_winner, 'inner') \
            .join(match_loser_df, matches.team1 == match_loser_df.match_loser, 'inner')

teams_df = team_df1.select(col('team1').alias('teams'), 'total_matches', 'wins', 'losses')

# Write the transformed data back to S3
venue_df.write.mode("overwrite").csv("s3://ipl-transformed-data/transformed_venue", header=True)
match_df.write.mode("overwrite").csv("s3://ipl-transformed-data/transformed_match", header=True)
teams_df.write.mode("overwrite").csv("s3://ipl-transformed-data/transformed_teams", header=True)

# Stop the SparkSession
spark.stop()