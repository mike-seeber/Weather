#!/usr/bin/python
import boto
import boto3
import json
import os
import re
import smtplib
import urllib2
import time
import yaml


def weather_firehose(credentials, firehose, cities):
    """Send cities data from Weather Underground to Firehose.
    Credentials: string required to access weather api
    Firehose: AWS firehose client
    Cities: List of cities to obtain weather data"""

    # Beginning of api url
    url_start = 'http://api.wunderground.com/api/' + credentials + \
                '/almanac/astronomy/conditions/geolookup/hourly10day/q/'

    # List for speed_layer data
    speed_layer_data = []

    # Obtain weather for each city and send to firehose
    for city in cities:
        city_url = url_start + city + '.json'
        city_file = urllib2.urlopen(city_url)
        city_string = city_file.read()
        city_file.close
        city_json = json.loads(city_string)

        # Send to firehose
        response = firehose.put_record(
                    DeliveryStreamName='weather3',
                    Record={
                        'Data': json.dumps(city_json)+'\n'
                    }
        )

        # Store city, current temp for Speed Layer
        current_observation = city_json.get('current_observation')
        if current_observation:
            display_location = current_observation.get('display_location')
            temp = current_observation.get('temp_f')
            if display_location and temp:
                city = display_location.get('city')
                if city:
                    speed_layer_data.append((city, temp))

        time.sleep(10)

    return speed_layer_data


def speed_layer_updates(speed_layer_data):
    """Update web_site with current temperatures"""
    index = bucket.get_key('index.html')
    index_str = index.get_contents_as_string()
    for record in speed_layer_data:
        city, temp = record
        city_formatted = city.replace(' ', '').lower()

        # Update index
        index_pattern = '\d?\d?\d\.\d&deg;F&nbsp;&nbsp;<a href="' +\
            city_formatted
        index_replace = str(temp) + '&deg;F&nbsp;&nbsp;<a href="' +\
            city_formatted
        index_str = re.sub(index_pattern, index_replace, index_str)

        # Update city page
        city_page = bucket.get_key(city_formatted + '.html')
        city_str = city_page.get_contents_as_string()
        city_pattern = '<h2>\d?\d?\d\.\d&deg;F</h2>'
        city_replace = '<h2>' + str(temp) + '&deg;F</h2>'
        city_str = re.sub(city_pattern, city_replace, city_str)

        city_page.set_contents_from_string(city_str, policy='public-read')

    index.set_contents_from_string(index_str, policy='public-read')

    return len(speed_layer_data)


def send_email(cities_updated):
    """Send completion e-mail"""
    sender = 'student.seeber@galvanize.it'
    receivers = ['mseeber101@gmail.com']
    header = 'To: ' + receivers[0] + '\n' + 'From: ' + sender + '\n' + \
        'MIME-Version: 1.0\nContent-type: text/html \nSubject: Weather Job\n'
    message = header + """
    <!DOCTYPE html>
    <html>
    Weather Job Complete <br>
    Cities Updated: """ + str(cities_updated) + """
    </html>
    """
    smtpObj = smtplib.SMTP('localhost')
    smtpObj.sendmail(sender, receivers, message)

if __name__ == '__main__':
    # Obtain credentials for weather api
    credentials_loc = '../WeatherUnderground.yml'
    credentials = yaml.load(open(os.path.expanduser(credentials_loc)))['WUKey']

    # Instantiate firehose client
    firehose = boto3.client('firehose', region_name='us-east-1')

    # Connect to bucket for speed_layer update
    conn = boto.connect_s3(host='s3.amazonaws.com')
    bucket_name = 'myweatherproject'
    bucket = conn.get_bucket(bucket_name)

    # Define city list
    cities = ['AK/Anchorage',
              'AZ/Phoenix',
              'CA/San_Francisco',
              'CO/Denver',
              'FL/Orlando',
              'ID/Boise',
              'IL/Chicago',
              'MA/Boston',
              'MN/Minneapolis',
              'NE/Omaha',
              'NV/Las_Vegas',
              'SC/Myrtle_Beach',
              'TN/Nashville',
              'TX/Austin',
              'WA/Seattle']

    # Send weather data to firehose and collect speed layer data
    speed_layer_data = weather_firehose(credentials, firehose, cities)

    # Update website with current temp
    cities_updated = speed_layer_updates(speed_layer_data)

    # Send completion e-mail
    send_email(cities_updated)
