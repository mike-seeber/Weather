import boto
import datetime
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
import smtplib


def timestamp(date, time):
    """Convert date and time to timestamp"""
    return (datetime.datetime.strptime(date + time, '%Y-%m-%d%H:%M') -
            datetime.datetime(1970, 1, 1)).total_seconds()


def timestamp_hour(date, time):
    """Convert date and time to timestamp of the hour (truncate minutes)"""
    dt = datetime.datetime.strptime(date + time, '%Y-%m-%d%H:%M')
    return (datetime.datetime(dt.year, dt.month, dt.day, dt.hour, 0) -
            datetime.datetime(1970, 1, 1)).total_seconds()


def hour_dif(date, time, fdate, ftime):
    """Hours from date/time to future date/time"""
    return (timestamp_hour(fdate, ftime) - timestamp_hour(date, time))/3600


def load():
    """Load dataframes"""
    cityDF = spark.read.parquet("cityDF.parquet").cache()
    cityDayDF = spark.read.parquet("cityDayDF.parquet").cache()
    weatherDF = spark.read.parquet("weatherDF.parquet").cache()
    forecastDF = spark.read.parquet("forecastDF.parquet").cache()
    statsDF = spark.read.parquet("statsDF.parquet").cache()

    # Sql Views
    cityDF.createOrReplaceTempView('cityV')
    cityDayDF.createOrReplaceTempView('cityDayV')
    weatherDF.createOrReplaceTempView('weatherV')
    forecastDF.createOrReplaceTempView('forecastV')
    statsDF.createOrReplaceTempView('statsV')

    return cityDF, cityDayDF, weatherDF, forecastDF, statsDF


def preprocess():
    """Preprocess dataframes for use with all cities"""
    # Keep only most recent day for each city
    cityDay1DF = spark.sql(
        "select * \
        from \
            (select * \
                , row_number() over(partition by city \
                    order by date desc) as rk \
                from cityDayV) \
            where rk=1").drop('rk')

    # Keep only most recent day/time for each city
    weather1DF = spark.sql(
        "select * \
        from \
            (select * \
                , row_number() over(partition by city \
                    order by cast(timestamp(date, time) as int) desc) as rk \
                from weatherV) \
            where rk=1").drop('rk')

    # Keep only most recent day/time for each city
    forecast1DF = spark.sql(
        "select * \
        ,cast(hour_dif(date, time, fdate, ftime) as int) as fhours \
        ,cast(timestamp(fdate, ftime) as int) as fts \
        from \
            (select * \
                , row_number() over(partition by city \
                    order by cast(timestamp(date, time) as int) desc) as rk \
                from forecastV) \
            where rk<=240").drop('rk')

    forecast1DF.createOrReplaceTempView('forecast1V')
    forecast2DF = spark.sql(
        "select \
        f.city \
        ,f.fts \
        ,cast(f.temp as int) as temp \
        ,cast(f.humidity as int) as humidity \
        ,cast(f.wind_mph as int) as wind_mph \
        ,cast(f.feelslike as int) as feelslike \
        ,s.tempSD \
        ,s.humiditySD \
        ,s.wind_mphSD \
        ,s.feelslikeSD \
        from forecast1V f, statsV s \
        where f.fhours = s.fhours")

    # Cache since you will reuse with each city
    cityDay1DF.cache()
    weather1DF.cache()
    forecast2DF.cache()

    return cityDay1DF, weather1DF, forecast2DF


def city_stats(city):
    """Obtain City Stats HTML"""
    cityP = spark.sql(
            "select \
            city \
            ,state \
            ,country \
            ,elevation \
            ,latitude \
            ,longitude \
            ,local_tz_offset \
            from cityV \
            where city = '" + city + "'").toPandas().T
    cityP.index = [i.replace('_', ' ').title() for i in cityP.index]
    cityHTML = cityP.to_html(header=False).replace('\n', '')
    return cityHTML


def cityDay_stats(city):
    """Obtain City/Day Stats HTML"""
    normal_high, \
        record_high, \
        record_high_year, \
        normal_low, \
        record_low, \
        record_low_year, \
        sunrise_hour, \
        sunrise_minute, \
        sunset_hour, \
        sunset_minute, \
        moonrise_hour, \
        moonrise_minute, \
        moonset_hour, \
        moonset_minute = cityDay1DF.filter(cityDay1DF['city'] == city).select(
            'normal_high',
            'record_high',
            'record_high_year',
            'normal_low',
            'record_low',
            'record_low_year',
            'sunrise_hour',
            'sunrise_minute',
            'sunset_hour',
            'sunset_minute',
            'moonrise_hour',
            'moonrise_minute',
            'moonset_hour',
            'moonset_minute').take(1)[0]

    histP = pd.DataFrame(
        {'Normal': [normal_low, normal_high],
         'Record': [record_low, record_high],
         'Record Year': [record_low_year, record_high_year]})
    histP.index = ['Low', 'High']
    histHTML = histP.to_html(header=True).replace('\n', '')

    moonset = '' if (moonset_hour + ':' + moonset_minute) == ':' else \
        (moonset_hour + ':' + moonset_minute)
    sunMoonP = pd.DataFrame(
        {'Sun': [sunrise_hour + ':' + sunrise_minute,
                 sunset_hour + ':' + sunset_minute],
         'Moon': [moonrise_hour + ':' + moonrise_minute,
                  moonset]}) \
        .T.sort_index(ascending=False)
    sunMoonP.columns = ['Rise', 'Set']
    sunMoonHTML = sunMoonP.to_html(header=True).replace('\n', '')

    return histHTML, sunMoonHTML


def forecast_stats(city):
    # Forecast to Pandas Dataframe with Needed Columns
    forecastP = forecast2DF.filter(forecast2DF['city'] == city) \
        .orderBy('fts').toPandas()
    forecastP['Date'] = forecastP.fts.apply(
        lambda x: datetime.datetime.fromtimestamp(x))
    forecastP['fts2'] = forecastP.fts.apply(
        lambda x: datetime.datetime.strftime(
            datetime.datetime.fromtimestamp(x), "%m/%d %H:00"))
    forecastP['temp_lower'] = forecastP['temp'] - 1.96 * forecastP['tempSD']
    forecastP['temp_upper'] = forecastP['temp'] + 1.96 * forecastP['tempSD']
    forecastP['humidity_lower'] = forecastP['humidity'] - \
        1.96 * forecastP['humiditySD']
    forecastP['humidity_upper'] = forecastP['humidity'] + \
        1.96 * forecastP['humiditySD']
    forecastP['humidity_lower'] = forecastP['humidity_lower'].apply(
        lambda x: max(0, x))
    forecastP['humidity_upper'] = forecastP['humidity_upper'].apply(
        lambda x: min(100, x))
    forecastP['wind_mph_lower'] = forecastP['wind_mph'] - \
        1.96 * forecastP['wind_mphSD']
    forecastP['wind_mph_upper'] = forecastP['wind_mph'] + \
        1.96 * forecastP['wind_mphSD']
    forecastP['wind_mph_lower'] = forecastP['wind_mph_lower'].apply(
        lambda x: max(0, x))
    forecastP['feelslike_lower'] = forecastP['feelslike'] - \
        1.96 * forecastP['feelslikeSD']
    forecastP['feelslike_upper'] = forecastP['feelslike'] + \
        1.96 * forecastP['feelslikeSD']

    # Create Plots (png files) and Save to S3
    # Temp
    fimage = forecastP.plot(x='Date',
                            y=['temp_lower', 'temp', 'temp_upper'],
                            legend=False,
                            title='Forecast Temperature',
                            figsize=(12, 6),
                            style=['k--', 'b-', 'k--']).get_figure()
    filepath = 'images/' + city + 'Temp.png'
    fimage.savefig(filepath)
    index_key = bucket.new_key(filepath)
    index_key.set_contents_from_filename(filepath, policy='public-read')
    # Humidity
    fimage = forecastP.plot(x='Date',
                            y=['humidity_lower', 'humidity', 'humidity_upper'],
                            legend=False,
                            title='Forecast Humidity',
                            figsize=(12, 6),
                            style=['k--', 'b-', 'k--']).get_figure()
    filepath = 'images/' + city + 'Hum.png'
    fimage.savefig(filepath)
    index_key = bucket.new_key(filepath)
    index_key.set_contents_from_filename(filepath, policy='public-read')
    # Wind
    fimage = forecastP.plot(x='Date',
                            y=['wind_mph_lower', 'wind_mph', 'wind_mph_upper'],
                            legend=False,
                            title='Forecast Wind Speed',
                            figsize=(12, 6),
                            style=['k--', 'b-', 'k--']).get_figure()
    filepath = 'images/' + city + 'Wind.png'
    fimage.savefig(filepath)
    index_key = bucket.new_key(filepath)
    index_key.set_contents_from_filename(filepath, policy='public-read')
    # Feelslike
    fimage = forecastP.plot(x='Date',
                            y=['feelslike_lower', 'feelslike',
                               'feelslike_upper'],
                            legend=False,
                            title='Forecast "Feels Like"',
                            figsize=(12, 6),
                            style=['k--', 'b-', 'k--']).get_figure()
    filepath = 'images/' + city + 'Feel.png'
    fimage.savefig(filepath)
    index_key = bucket.new_key(filepath)
    index_key.set_contents_from_filename(filepath, policy='public-read')

    # Close all figures so you don't have too many open at once!
    plt.close('all')

    # Create Forecast Table
    forecast1P = forecastP[['fts2', 'temp', 'humidity', 'wind_mph',
                            'feelslike']].set_index('fts2')
    del forecast1P.index.name
    forecast1P.columns = ['Temperature', 'Humidity', 'Wind Speed',
                          'Feels Like']
    forecastHTML = forecast1P.to_html(header=True).replace('\n', '')

    return forecastHTML


def city_html(city, cityHTML, histHTML, sunMoonHTML, current_temp,
              forecastHTML):
    html = '<!DOCTYPE html><html><h1>' +\
            city + '</h1><h2>' +\
            str(current_temp) + '&deg;F' +\
            '</h2><h3>City Stats</h3><p>' +\
            cityHTML +\
            '</p><h3>Sun & Moon Schedule</h3><p>' +\
            sunMoonHTML +\
            '</p><h3>Today\'s Historical Temperatures</h3><p>' +\
            histHTML +\
            '</p><h3>10-Day Forecast</h3><p>' +\
            '</p><h4>95% Confidence Interval Plots</h4><p>' +\
            '<img src="images\\' + city + 'Temp.png" \
            style="width:800px;height:400px;">' +\
            '<img src="images\\' + city + 'Hum.png" \
            style="width:800px;height:400px;">' +\
            '<img src="images\\' + city + 'Wind.png" \
            style="width:800px;height:400px;">' +\
            '<img src="images\\' + city + 'Feel.png" \
            style="width:800px;height:400px;">' +\
            '</p><h4>Table of Point Estimates</h4><p>' +\
            forecastHTML +\
            '</p></html>'

    city_key = bucket.new_key(city.lower().replace(' ', '') + '.html')
    city_key.content_type = 'text/html'
    city_key.set_contents_from_string(html, policy='public-read')
    return


def city_pages():
    """Create webpages for each city"""
    current_weather = []
    for c in sorted(cityDF.select('city').collect()):
        city = c['city']

        # City Stats
        cityHTML = city_stats(city)

        # Day Stats
        histHTML, sunMoonHTML = cityDay_stats(city)

        # Current Weather
        current_temp = weather1DF.filter(weather1DF['city'] == city)\
            .take(1)[0]['temp']
        current_weather.append((city, current_temp))

        # Forecast Weather
        forecastHTML = forecast_stats(city)

        # Create Web-Page on S3
        city_html(city, cityHTML, histHTML, sunMoonHTML, current_temp,
                  forecastHTML)

    current_weather = {row[0]: row[1] for row in current_weather}
    return current_weather


def index_html(current_weather):
    html = '<!DOCTYPE html><html><head><h1>WEATHER</h1></head>' +\
        '<body><p>Please select a city:<br><br>'
    for c in sorted(cityDF.select('city').collect()):
        city = c['city']
        html += '&nbsp;&nbsp;&nbsp;&nbsp;' + str(current_weather[city]) +\
            '&deg;F&nbsp;&nbsp;<a href="' + city.lower().replace(' ', '') +\
            '.html">' +\
            city + '</a><br>'
    html += '</p></body></html>'

    index_key = bucket.new_key('index.html')
    index_key.content_type = 'text/html'
    index_key.set_contents_from_string(html, policy='public-read')
    return


def error_html():
    html = '<!DOCTYPE html><html><head><h3>The page you are looking for' +\
        'does not exist.</h3></head><body><p>Please return to the' +\
        '<a href="index.html">Weather Homepage</a></p></body></html>'

    error_key = bucket.new_key('error.html')
    error_key.content_type = 'text/html'
    error_key.set_contents_from_string(html, policy='public-read')
    return


def send_email():
    sender = 'student.seeber@galvanize.it'
    receivers = ['mseeber101@gmail.com']
    header = 'To: ' + receivers[0] + '\n' + 'From: ' + sender + '\n' + \
        'MIME-Version: 1.0\nContent-type: text/html \nSubject: Report Job\n'
    message = header + """
    <!DOCTYPE html>
    <html>
    Report Job Complete
    </html>
    """
    smtpObj = smtplib.SMTP('localhost')
    smtpObj.sendmail(sender, receivers, message)
    return


if __name__ == "__main__":
    # Start Spark
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Connect to bucket
    conn = boto.connect_s3(host='s3.amazonaws.com')
    bucket_name = 'myweatherproject'
    bucket = conn.get_bucket(bucket_name)

    # Register UDFs
    spark.udf.register("timestamp", timestamp)
    spark.udf.register("timestamp_hour", timestamp_hour)
    spark.udf.register("hour_dif", hour_dif)

    # Load DataFrames
    cityDF, cityDayDF, weatherDF, forecastDF, statsDF = load()

    # Preprocess DataFrames for Use with All Cities
    cityDay1DF, weather1DF, forecast2DF = preprocess()

    # Create Web-Pages for Each City and
    # Return Current Weather Dictionary to Use in Creating the Index
    current_weather = city_pages()

    # Create Index Web-Page
    index_html(current_weather)

    # Create Error Web-Page
    error_html()

    # Send Completion E-mail
    send_email()
