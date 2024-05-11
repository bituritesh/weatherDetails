import json
from dotenv import load_dotenv
import requests
import os
from loguru import logger
import sys
from dbConnection import cur, conn
from datetime import date

logger.remove()
console_format = ("<green>{time:YYYY-MM-DD HH:mm:ss}</green> |"
                  " <level>{level: <8}</level> |"
                  " <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
logger.add(sys.stderr, format=console_format, level="INFO", serialize=False)
logger.add(
    "app.log",
    serialize=True,
    level="DEBUG"
)
load_dotenv()
today = date.today()
logger.info(f"Today's date - {today}")


def area_geocode_with_name(baseUrl_for_geocode, area_geocode_with_name_fullData):
    geocodeApiFullPath = f"{baseUrl_for_geocode}{area_geocode_with_name_fullData}"
    # Log the request body
    logger.debug(f"Request Body: {area_geocode_with_name_fullData}")
    # Make the request
    response = requests.get(geocodeApiFullPath)
    # Log the response
    logger.debug(f"Response Body: {response.text}")
    return response


def areawise_weather(baseUrl_for_weather_data, weather_for_geocode_location_fulldata):
    areaWiseWeatherApiFullPath = f"{baseUrl_for_weather_data}{weather_for_geocode_location_fulldata}"
    logger.debug(f"Request Body: {weather_for_geocode_location_fulldata}")
    response = requests.get(areaWiseWeatherApiFullPath)
    logger.debug(f"Response Body: {response.text}")
    return response


try:
    baseUrl_for_geocode = os.environ["GEO_CODE_API_BASEURL"]
    zipCodes = json.loads(os.environ["ZIP_CODE"])
    countryCode = os.environ["COUNTRY_CODE"]
    appId = os.environ["OPEN_WEATHER_APP_ID"]
    for zip in zipCodes:
    #     # if zip weather data is already present in the weather table for current date then we will not make the api
    #     # calls and move to the next iteration.
    #     cur.execute("""SELECT MAX(date) AS latest_date
    #                     FROM Weather
    #                     WHERE pincode = '%s';""", (zip,))
    #     db_latest_date = cur.fetchone()[0]
    #     if db_latest_date == today:
    #         logger.info(f"iss pincode {zip} ka data aaj ka tarikh {db_latest_date} ka already hain, dobara API call "
    #                     f"NHI krne ka!!!")
    #     else:
        area_geocode_with_name_fullData = f"zip={zip},{countryCode}&appid={appId}"
        response = area_geocode_with_name(baseUrl_for_geocode, area_geocode_with_name_fullData)
        if response.status_code == 200:
            r = response.json()
            logger.success(f"geocode API response - {r}")
            for value in range(len(r)):
                locationDetails = [r.get("lat", "-"), r.get("lon", "-")]
            if "-" not in locationDetails:
                try:
                    baseUrl_for_weather_data = os.environ["WEATHER_FROM_GEOCODE_BASEURL"]
                    lat = locationDetails[0]
                    lon = locationDetails[1]
                    units = os.environ["WEATHER_DATA_UNITS"]
                    weather_for_geocode_location_fulldata = f"lat={lat}&lon={lon}&appid={appId}&units={units}"
                    weather_data = areawise_weather(baseUrl_for_weather_data, weather_for_geocode_location_fulldata)
                    if weather_data.status_code == 200:
                        weather_data_response = weather_data.json()
                        logger.success(f"weather data response  - {weather_data_response}")
                        current_date = today.strftime("%b-%d-%Y")
                        weather_data_response["main"]["zip"] = r.get("zip")
                        weather_data_response["main"]["name"] = r.get("name")
                        weather_data_response["main"]["time"] = current_date
                        weather_data_response["main"]["description"] = weather_data_response["weather"][0][
                            "description"]
                        weather_information = weather_data_response["main"]
                        try:
                            # Check if the pincode exists in the Pincode table
                            cur.execute("""SELECT pincode FROM Pincode WHERE pincode = %s;""", (weather_information[
                                                                                                    "zip"],))
                            pincode_exists = cur.fetchone()
                            if not pincode_exists:
                                # If the pincode does not exist, insert it into the Pincode table
                                cur.execute("INSERT INTO Pincode (pincode,city) VALUES (%s,%s)",
                                            (weather_information["zip"], weather_information["name"],))
                                conn.commit()
                                logger.info(f"Pincode {weather_information['zip']} and place name"
                                            f" {weather_information['name']} inserted into Pincode table.")
                            else:
                                logger.info(f"Pincode {weather_information['zip']} "
                                            f"already exists in Pincode table.")

                            # check if the weather description exists in the Weather_description table
                            cur.execute("""SELECT description FROM Weather_description WHERE description = %s;""",
                                        (weather_information["description"],))
                            weather_description = cur.fetchone()
                            if not weather_description:
                                # If the description do not exist, Insert into the Weather_description table
                                cur.execute("INSERT INTO Weather_description (description) VALUES (%s)",
                                            (weather_information["description"],))
                                conn.commit()
                                logger.info(f"{weather_information['description']} inserted into "
                                            f"Weather_description table.")
                            cur.execute("""SELECT description_id FROM Weather_description WHERE description = %s;""",
                                        (weather_information["description"],))
                            weather_description_id = cur.fetchone()
                            cur.execute("""INSERT INTO Weather (pincode,temp,feels_Like,humidity,description_id)
                                 VALUES (%s, %s, %s, %s, %s);""",
                                        (weather_information['zip'],
                                         weather_information['temp'],
                                         weather_information['feels_like'],
                                         weather_information['humidity'],
                                         weather_description_id,
                                         ))
                            conn.commit()
                            logger.info(f"Value inserted into Weather table.")
                        except Exception as e:
                            logger.error(f"Postgres error: {e}")
                    else:
                        logger.error(f"resp_body:{weather_data.status_code}")
                except Exception as e:
                    logger.error(f"area wise_weather error: {e}")
            else:
                logger.warning("Geocode not available for the entered pincode")
        else:
            logger.error(f"resp_body:{response.status_code}", exc_info=True)
except Exception as e:
    logger.error(f"area_geocode_with_name error: {e}")
finally:
    cur.close()
    conn.close()
# https://www.jcchouinard.com/python-automation-with-cron-on-mac/