import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, mean, stddev
import smtplib


def timestamp_hour(date, time):
    """Convert date and time to timestamp of the hour (truncate minutes)"""
    dt = datetime.datetime.strptime(date + time, '%Y-%m-%d%H:%M')
    return (datetime.datetime(dt.year, dt.month, dt.day, dt.hour, 0) -
            datetime.datetime(1970, 1, 1)).total_seconds()


def hour_dif(date, time, fdate, ftime):
    """Hours from date/time to future date/time"""
    return (timestamp_hour(fdate, ftime) - timestamp_hour(date, time))/3600


def convert_humidity(h):
    """Eliminate % from text so you can cast as int"""
    return h.replace('%', '')


def load():
    """Load dataframes"""
    weather1DF = spark.read.parquet("weatherDF.parquet").cache()
    forecast1DF = spark.read.parquet("forecastDF.parquet").cache()

    # Views
    weather1DF.createOrReplaceTempView('weather1V')
    forecast1DF.createOrReplaceTempView('forecast1V')

    return weather1DF, forecast1DF


def join():
    # Add timestamp hour (truncate minutes) to weather and forecast
    weather2DF = spark.sql(
        'select * \
        ,cast(timestamp_hour(date, time) as int) as ts \
        from weather1V')
    forecast2DF = spark.sql(
        'select * \
        ,cast(hour_dif(date, time, fdate, ftime) as int) as fhours \
        ,cast(timestamp_hour(fdate, ftime) as int) as fts \
        from forecast1V')
    weather2DF.createOrReplaceTempView('weather2V')
    forecast2DF.createOrReplaceTempView('forecast2V')

    # Join forecast weather with the actual weather that occurred
    join1DF = spark.sql(
        'select f.* \
        ,w.ts \
        ,w.temp as tempA \
        ,w.humidity as humidityA \
        ,w.wind_mph as wind_mphA \
        ,w.feelslike as feelslikeA \
        from forecast2V f, weather2V w \
        where f.city = w.city and f.fts = w.ts')
    join1DF.createOrReplaceTempView('join1V')

    # Calculate Actual minus Forecast
    join2DF = spark.sql(
        'select \
        city \
        ,fhours \
        ,tempA - cast(temp as int) as tempD \
        ,cast(convert_humidity(humidityA) as int) - \
            cast(humidity as int) as humidityD \
        ,wind_mphA - cast(wind_mph as int) as wind_mphD \
        ,cast(feelslikeA as int) - cast(feelslike as int) as feelslikeD \
        from join1V')
    return join2DF


def stats():
    stats1DF = joinDF.orderBy('fhours').groupBy('fhours').agg(
        count('fhours').alias('count'),
        mean('tempD').alias('tempM'),
        stddev('tempd').alias('tempSD'),
        mean('humidityD').alias('humidityM'),
        stddev('humidityD').alias('humiditySD'),
        mean('wind_mphD').alias('wind_mphM'),
        stddev('wind_mphD').alias('wind_mphSD'),
        mean('feelslikeD').alias('feelslikeM'),
        stddev('feelslikeD').alias('feelslikeSD'))
    return stats1DF


def write_parquet():
    """Write stats DF to parquet"""
    statsDF.write.parquet("statsDF.parquet", mode='overwrite')
    return


def send_email():
    sender = 'student.seeber@galvanize.it'
    receivers = ['mseeber101@gmail.com']
    header = 'To: ' + receivers[0] + '\n' + 'From: ' + sender + '\n' + \
        'MIME-Version: 1.0\nContent-type: text/html \nSubject: Analyze Job\n'
    message = header + """
    <!DOCTYPE html>
    <html>
    Analyze Job Complete
    </html>
    """
    smtpObj = smtplib.SMTP('localhost')
    smtpObj.sendmail(sender, receivers, message)
    return

if __name__ == "__main__":
    # Start Spark
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Register UDFs
    spark.udf.register("timestamp_hour", timestamp_hour)
    spark.udf.register("hour_dif", hour_dif)
    spark.udf.register("convert_humidity", convert_humidity)

    # Load Data
    weather1DF, forecast1DF = load()

    # Join forecast weather with the actual weather that happened
    joinDF = join()

    # Stats by forecast lag
    statsDF = stats()

    # Write stats DF to parquet
    write_parquet()

    # Send Completion E-mail
    send_email()
