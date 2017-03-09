import boto
import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, lit
import smtplib


def date_from_epoch(epoch, offset):
    """Return YYYY-MM-DD from timestamp (epoch) and offset"""
    if epoch is None or offset is None:
        return None
    dt = datetime.datetime.fromtimestamp(epoch) + \
        datetime.timedelta(hours=offset/100)
    return datetime.datetime.strftime(dt, '%Y-%m-%d')


def time_from_epoch(epoch, offset):
    """Return HH:MM from timestamp (epoch) and offset"""
    if epoch is None or offset is None:
        return None
    dt = datetime.datetime.fromtimestamp(epoch) + \
        datetime.timedelta(hours=offset/100)
    return datetime.datetime.strftime(dt, '%H:%M')


def myzip(*cols):
    """Zip columns function used to zip forecast lists together"""
    return zip(*cols)


def myreshape(a, i=4):
    """Flatten ['0', '1', '2', '3', ['4a', '4b', '4c']] to
    onel list ['0', '1', '2', '3','4a', '4b', '4c']"""
    start = list(a[:i])
    end = a[i]
    return [start + list(x) for x in end]


def path_s3():
    """Obtain all raw data filepaths from S3"""
    keys = bucket.get_all_keys()
    pathS3 = []
    for k in keys:
        if ('weather-1' in k.name) or ('weather3-4' in k.name):
            pathS3.append((k.name, ))
    pathS3DF = spark.sparkContext.parallelize(pathS3).toDF() \
        .selectExpr('_1 as path')
    return pathS3DF


def path_to_dfs(path):
    """Given a filepath, extract and normalize data into DFs"""
    path_text = path[0]['path']

    # Source of file where the data came from
    source = int('2017'+path_text[path_text.find('/')+1:path_text.find('/w')]
                 .replace('/', ''))

    # Raw Data / Clean / Cache
    pathRawDF = spark.read.json('s3a://myweatherproject/' + path_text)
    pathCleanDF = pathRawDF.filter(
        pathRawDF['current_observation.display_location.city'] != '')
    pathCleanDF.cache()

    # Table = city, Key = city
    # Information about the city
    pathCityDF = pathCleanDF.selectExpr(
        'current_observation.display_location.city',
        'current_observation.display_location.state',
        'current_observation.display_location.country',
        'current_observation.display_location.zip',
        'almanac.airport_code',
        'current_observation.display_location.elevation',
        'current_observation.display_location.latitude',
        'current_observation.display_location.longitude',
        'current_observation.local_tz_offset'
        ).distinct().withColumn('source', lit(source))

    # Table = nearby, Key = city, nearby
    # For each city, contains all the nearby locations.
    pathNearby1DF = pathCleanDF.selectExpr(
        'current_observation.display_location.city',
        'explode(location.nearby_weather_stations.pws.station.neighborhood) \
            as nearby') \
        .unionAll(pathCleanDF.selectExpr(
            'current_observation.display_location.city',
            'explode(location.nearby_weather_stations.airport.station.city) \
            as nearby'
            )).distinct()
    pathNearbyDF = pathNearby1DF.filter((pathNearby1DF['nearby'] != '')) \
        .withColumn('source', lit(source))

    # Table = cityDay, Key = ciy, date
    # Stats for the given key: historcial temps, sun/moon schedule.
    pathCityDayDF = pathCleanDF.selectExpr(
        'current_observation.display_location.city',
        'date_from_epoch(cast(current_observation.local_epoch as int),\
             cast(current_observation.local_tz_offset as int)) as date',
        'almanac.temp_high.normal.F as normal_high',
        'almanac.temp_high.record.F as record_high',
        'almanac.temp_high.recordyear as record_high_year',
        'almanac.temp_low.normal.F as normal_low',
        'almanac.temp_low.record.F as record_low',
        'almanac.temp_low.recordyear as record_low_year',
        'sun_phase.sunrise.hour as sunrise_hour',
        'sun_phase.sunrise.minute as sunrise_minute',
        'sun_phase.sunset.hour as sunset_hour',
        'sun_phase.sunset.minute as sunset_minute',
        'moon_phase.moonrise.hour as moonrise_hour',
        'moon_phase.moonrise.minute as moonrise_minute',
        'moon_phase.moonset.hour as moonset_hour',
        'moon_phase.moonset.minute as moonset_minute'
        ).distinct().withColumn('source', lit(source))

    # Table = weather, Key = city, date, time
    # Current weather data for the given key.
    pathWeatherDF = pathCleanDF.selectExpr(
        'current_observation.display_location.city',
        'date_from_epoch(cast(current_observation.local_epoch as int),\
             cast(current_observation.local_tz_offset as int)) as date',
        'time_from_epoch(cast(current_observation.local_epoch as int),\
             cast(current_observation.local_tz_offset as int)) as time',
        'current_observation.temp_f as temp',
        'current_observation.feelslike_f as feelslike',
        'current_observation.windchill_f as windchill',
        'current_observation.dewpoint_f as dewpoint',
        'current_observation.UV',
        'current_observation.precip_1hr_in as precip',
        'current_observation.pressure_in as pressure',
        'current_observation.relative_humidity as humidity',
        'current_observation.visibility_mi',
        'current_observation.wind_dir',
        'current_observation.wind_gust_mph',
        'current_observation.wind_mph'
        ).distinct().withColumn('source', lit(source))

    # Table = forecast, Key = city, date, time, fdate, ftime
    # fdate/ftime are forecast date/time
    # Forecast weather data for the given key
    # Step 1: Convert current epochs to date/time and switch to RDD
    pathForecast1RDD = pathCleanDF.selectExpr(
        'current_observation.display_location.city',
        'date_from_epoch(cast(current_observation.local_epoch as int),\
             cast(current_observation.local_tz_offset as int)) as date',
        'time_from_epoch(cast(current_observation.local_epoch as int),\
             cast(current_observation.local_tz_offset as int)) as time',
        'cast(current_observation.local_tz_offset as int) as offset',
        'hourly_forecast.FCTTIME.epoch as epoch',
        'hourly_forecast.temp.english as temp',
        'hourly_forecast.feelslike.english as feelslike',
        'hourly_forecast.windchill.english as windchill',
        'hourly_forecast.dewpoint.english as dewpoint',
        'hourly_forecast.uvi as UV',
        'hourly_forecast.humidity as humidity',
        'hourly_forecast.wdir.dir as wind_dir',
        'hourly_forecast.wspd.english as wind_mph',
        ).rdd
    # Step 2: Zip forecast stats lists into 1 list of tuples
    pathForecast2RDD = pathForecast1RDD.map(lambda x: (
        x['city'],
        x['date'],
        x['time'],
        x['offset'],
        myzip(
            x['epoch'],
            x['temp'],
            x['feelslike'],
            x['windchill'],
            x['dewpoint'],
            x['UV'],
            x['humidity'],
            x['wind_dir'],
            x['wind_mph'])))
    # Step 3: Reshape and flatten into 1 row per key
    pathForecast3RDD = pathForecast2RDD.map(myreshape).flatMap(lambda x: x)
    # Step 4: From RDD back to DF
    pathForecast1DF = spark.createDataFrame(pathForecast3RDD, [
        'city',
        'date',
        'time',
        'offset',
        'epoch',
        'temp',
        'feelslike',
        'windchill',
        'dewpoint',
        'UV',
        'humidity',
        'wind_dir',
        'wind_mph'])
    # Step 5: Convert future epochs to fdate/ftime
    pathForecastDF = pathForecast1DF.selectExpr(
        'city',
        'date',
        'time',
        'date_from_epoch(cast(epoch as int),\
             cast(offset as int)) as fdate',
        'time_from_epoch(cast(epoch as int),\
             cast(offset as int)) as ftime',
        'temp',
        'feelslike',
        'windchill',
        'dewpoint',
        'UV',
        'humidity',
        'wind_dir',
        'wind_mph').withColumn('source', lit(source))

    return pathCityDF, pathNearbyDF, pathCityDayDF, pathWeatherDF, \
        pathForecastDF


def load_or_create():
    """Load parquet files if they exist.
    Otherwise, create DFs from first S3 file
    """
    # Try to load all of the existing dataframes
    try:
        path1DF = spark.read.parquet("pathDF.parquet").cache()
        city1DF = spark.read.parquet("cityDF.parquet").cache()
        nearby1DF = spark.read.parquet("nearbyDF.parquet").cache()
        cityDay1DF = spark.read.parquet("cityDayDF.parquet").cache()
        weather1DF = spark.read.parquet("weatherDF.parquet").cache()
        forecast1DF = spark.read.parquet("forecastDF.parquet").cache()

        # Cache DF so your first action isn't trying to overwrite these files
        a, b, c, d, e, f = path1DF.count(), city1DF.count(), \
            nearby1DF.count(), cityDay1DF.count(), weather1DF.count(), \
            forecast1DF.count()
    # Otherwise, create the needed dataframes from scratch using 1 path
    except:
        path = pathS3DF.take(1)
        path1DF = spark.sparkContext.parallelize(path).toDF()
        city1DF, nearby1DF, cityDay1DF, weather1DF, forecast1DF = \
            path_to_dfs(path)
    return path1DF, city1DF, nearby1DF, cityDay1DF, weather1DF, forecast1DF


def load_or_create_troubleshoot():
    """To troubleshoot, replace the load_or_create function in main
    This forces recreation of dataframes from scratch"""
    path = pathS3DF.take(1)
    pathDF = spark.sparkContext.parallelize(path).toDF()
    cityDF, nearbyDF, cityDayDF, weatherDF, forecastDF = path_to_dfs(path)
    return pathDF, cityDF, nearbyDF, cityDayDF, weatherDF, forecastDF


def append_new():
    """Append new data from paths in pathS3DF that aren't in pathDF"""
    # Identify new paths
    pathS3DF.createOrReplaceTempView('pathS3V')
    path1DF.createOrReplaceTempView('path1V')
    newPathDF = spark.sql(
        'select \
        path \
        from pathS3V \
        where path not in (select path from path1V)')

    # Stop if no new paths
    if newPathDF.count() == 0:
        return 0, 0, 0, 0, 0, 0, 0

    # Initialize output DFs
    path2DF = path1DF
    city2DF = city1DF
    nearby2DF = nearby1DF
    cityDay2DF = cityDay1DF
    weather2DF = weather1DF
    forecast2DF = forecast1DF

    # Append data for each new path to the respective dataframe
    appended_count = 0
    for path in newPathDF.collect()[0:100]:
        pathPathDF = spark.sparkContext.parallelize([path]).toDF()
        pathCityDF, pathNearbyDF, pathCityDayDF, pathWeatherDF, \
            pathForecastDF = path_to_dfs([path])

        path2DF = path2DF.unionAll(pathPathDF)
        city2DF = city2DF.unionAll(pathCityDF)
        nearby2DF = nearby2DF.unionAll(pathNearbyDF)
        cityDay2DF = cityDay2DF.unionAll(pathCityDayDF)
        weather2DF = weather2DF.unionAll(pathWeatherDF)
        forecast2DF = forecast2DF.unionAll(pathForecastDF)

        appended_count += 1

    return appended_count, path2DF, city2DF, nearby2DF, cityDay2DF, \
        weather2DF, forecast2DF


def enforce_keys():
    """Enforce keys by taking most recent record if duplicates"""
    city2DF.createOrReplaceTempView('city2V')
    nearby2DF.createOrReplaceTempView('nearby2V')
    cityDay2DF.createOrReplaceTempView('cityDay2V')
    weather2DF.createOrReplaceTempView('weather2V')
    forecast2DF.createOrReplaceTempView('forecast2V')

    # path - no change
    path3DF = path2DF
    # cityDF: city
    city3DF = spark.sql(
        'select * \
        from \
            (select * \
            ,row_number() over(partition by city order by source desc) as rk \
            from city2V) \
        where rk=1').drop('rk')
    # nearbyDF: city, nearby
    nearby3DF = spark.sql(
        'select * \
        from \
            (select * \
            ,row_number() over(partition by city, nearby \
                order by source desc) as rk \
            from nearby2V) \
        where rk=1').drop('rk')
    # cityDayDF: city, date
    cityDay3DF = spark.sql(
        'select * \
        from \
            (select * \
            ,row_number() over(partition by city, date \
                order by source desc) as rk \
            from cityDay2V) \
        where rk=1').drop('rk')
    # weatherDF: city, date, time
    weather3DF = spark.sql(
        'select * \
        from \
            (select * \
            ,row_number() over(partition by city, date, time \
                order by source desc) as rk \
            from weather2V) \
        where rk=1').drop('rk')
    # forecastDF: city, date, time, fdate, ftime
    forecast3DF = spark.sql(
        'select * \
        from \
            (select * \
            , row_number() over(partition by city, date, time, fdate, ftime \
                order by source desc) as rk \
            from forecast2V) \
        where rk=1').drop('rk')

    return path3DF, city3DF, nearby3DF, cityDay3DF, weather3DF, forecast3DF


def write_parquet():
    """Write DFs to parquet for re-use next time"""
    # Cache output files for continued use in analysis.
    path3DF.cache()
    city3DF.cache()
    nearby3DF.cache()
    cityDay3DF.cache()
    weather3DF.cache()
    forecast3DF.cache()

    # Write files
    path3DF.write.parquet("pathDF.parquet", mode='overwrite')
    city3DF.write.parquet("cityDF.parquet", mode='overwrite')
    nearby3DF.write.parquet("nearbyDF.parquet", mode='overwrite')
    cityDay3DF.write.parquet("cityDayDF.parquet", mode='overwrite')
    weather3DF.write.parquet("weatherDF.parquet", mode='overwrite')
    forecast3DF.write.parquet("forecastDF.parquet", mode='overwrite')

    # Uncache read-in DFs so you read from file next time
    path1DF.unpersist()
    city1DF.unpersist()
    nearby1DF.unpersist()
    cityDay1DF.unpersist()
    weather1DF.unpersist()
    forecast1DF.unpersist()
    return


def send_email():
    sender = 'student.seeber@galvanize.it'
    receivers = ['mseeber101@gmail.com']
    header = 'To: ' + receivers[0] + '\n' + 'From: ' + sender + '\n' + \
        'MIME-Version: 1.0\nContent-type: text/html \nSubject: Normalize Job\n'
    message = header + """
    <!DOCTYPE html>
    <html>
    Normalize Job Complete <br>
    Appended Count: """ + str(appended_count) + """
    </html>
    """
    smtpObj = smtplib.SMTP('localhost')
    smtpObj.sendmail(sender, receivers, message)
    return


if __name__ == "__main__":
    ############################################################
    # Obtain New Data From S3, Append, Normalize, Write Parquet
    ############################################################
    # Start Spark
    conf = SparkConf()
    conf.set('spark.driver.maxResultSize', '5g')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Connect to bucket
    conn = boto.connect_s3(host='s3.amazonaws.com')
    bucket_name = 'myweatherproject'
    bucket = conn.get_bucket(bucket_name)

    # Register UDFs
    spark.udf.register("date_from_epoch", date_from_epoch)
    spark.udf.register("time_from_epoch", time_from_epoch)

    # New DF listing all raw data files in S3
    pathS3DF = path_s3()

    # Load Noramlized DFs if they exist, otherwise create from first path.
    path1DF, city1DF, nearby1DF, cityDay1DF, weather1DF, forecast1DF = \
        load_or_create()

    # Append data from all new paths to the DFs
    appended_count, path2DF, city2DF, nearby2DF, cityDay2DF, weather2DF, \
        forecast2DF = append_new()

    # If there were new paths appended then proceed
    if path2DF != 0:
        # After appending duplicates, time to enforce keys!
        path3DF, city3DF, nearby3DF, cityDay3DF, weather3DF, forecast3DF = \
            enforce_keys()

        # Write DF to parquet
        write_parquet()

    # Send Completion E-mail
    send_email()
