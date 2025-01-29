from flask import Flask, request, Response
import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timezone
import re
import openmeteo_requests
import requests_cache
from retry_requests import retry

app = Flask(__name__)

cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

DATA_DIR = "data/"

def build_search_response(results):
    query = ET.Element("query", {
        "xmlns:yahoo": "http://www.yahooapis.com/v1/base.rng",
        "yahoo:count": str(len(results)),
        "yahoo:created": datetime.now(timezone.utc).isoformat(),
        "yahoo:lang": "en-US"
    })
    results_elem = ET.SubElement(query, "results")

    for result in results:
        city = result.get("name", "")
        country = result.get("country_code", "")
        state = result.get("admin1_code", "")
        woeid = str(result.get("geonameid", "0"))

        city = city if pd.notna(city) else ""
        country = country if pd.notna(country) else ""
        state = state if pd.notna(state) else ""
        woeid = woeid if pd.notna(woeid) else "0"

        ET.SubElement(results_elem, "location", {
            "city": city,
            "country": country,
            "countryAbbr": country,
            "state": state,
            "stateAbbr": state,
            "locationID": "0000",
            "woeid": woeid
        })
    return ET.tostring(query, encoding="utf-8", xml_declaration=True).decode()

def handle_search_query(query_param):
    search_text = ""
    if 'where query=' in query_param:
        start_index = query_param.find('where query=') + len('where query=')
        end_index = query_param.find('"', start_index + 1)
        search_text = query_param[start_index:end_index].strip('"').strip()

    if not search_text:
        return "Invalid or empty query parameter", 400

    first_letter = search_text[0].upper()
    file_name = f"{first_letter}.txt" if "A" <= first_letter <= "Z" else "Misc.txt"
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        return f"No data file for letter '{first_letter}'", 404

    columns = [
        "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
        "feature_class", "feature_code", "country_code", "cc2", "admin1_code",
        "admin2_code", "admin3_code", "admin4_code", "population", "elevation",
        "dem", "timezone", "modification_date"
    ]
    try:
        data = pd.read_csv(file_path, sep="\t", header=None, names=columns, low_memory=False)
    except Exception as e:
        return f"Error reading data file: {e}", 500

    results = data[data["name"].str.contains(search_text, case=False, na=False)].to_dict(orient="records")

    xml_response = build_search_response(results)
    return Response(xml_response, content_type="application/xml")

def find_coordinates(query: str):
    match = re.search(r"woeid=(\d+)", query)
    if not match:
        raise ValueError("WOEID not found in the query string.")

    woeid = int(match.group(1))

    for file_name in os.listdir(DATA_DIR):
        file_path = os.path.join(DATA_DIR, file_name)
        if not os.path.isfile(file_path):
            continue

        columns = [
            "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
            "feature_class", "feature_code", "country_code", "cc2", "admin1_code",
            "admin2_code", "admin3_code", "admin4_code", "population", "elevation",
            "dem", "timezone", "modification_date"
        ]
        try:
            data = pd.read_csv(file_path, sep="\t", header=None, names=columns, low_memory=False)
        except Exception as e:
            print(f"Error reading data file '{file_name}': {e}")
            continue

        matching_row = data[data["geonameid"] == woeid]
        if not matching_row.empty:
            lat = matching_row.iloc[0]["latitude"]
            lon = matching_row.iloc[0]["longitude"]
            cityname = matching_row.iloc[0]["name"]
            return woeid, lat, lon, cityname

    raise ValueError(f"No matching WOEID {woeid} found in data files.")

def weather_code_converter(open_meteo_code):     ### Come back to this and use "Isday" to pick sun or moon icons. ALSO figure out the different icons
    #print(open_meteo_code)
    #Make better mapping. This sucks
    mapping = {
        0: 32,  # Clear sky -> Sun
        1: 30, 2: 30, 3: 27,  # Partly cloudy to overcast -> Sun & partly cloudy / Cloudy
        45: 23, 48: 23,  # Fog -> Haze
        51: 11, 53: 11, 55: 11,  # Drizzle -> Rain
        56: 25, 57: 25,  # Freezing Drizzle -> Ice
        61: 11, 63: 11, 65: 11,  # Rain -> Rain
        66: 25, 67: 25,  # Freezing Rain -> Ice
        71: 15, 73: 15, 75: 15,  # Snowfall -> Snow
        77: 13,  # Snow grains -> Flurries
        80: 39, 81: 39, 82: 39,  # Rain showers -> Sun & Rain
        85: 35, 86: 35,  # Snow showers -> Rain & Snow
        95: 0,  # Thunderstorm -> Lightning
        96: 17, 99: 17,  # Thunderstorm with hail -> Hail
    }
    
    return int(mapping.get(open_meteo_code, 27))

def get_day_number(date):
    #print(date)
    day_number = (date.weekday() + 1) % 7
    if day_number == 7:
        #print("0")
        return 0
    else:
        #print(day_number)
        return day_number

def extract_time(date):
    date = str(date)
    dt = datetime.fromisoformat(date)
    return dt.strftime('%H:%M')        

def hourly_pre(precipitation):
    if precipitation > 0.1:
        return precipitation
    else:
        return ""

def generateWeatherXML(data, ID, CityName):
    response = data
    current = response.Current()
    current_time = [current.Time()]
    current_12hr = [datetime.fromtimestamp(ts).strftime('%I:%M %p') for ts in current_time]
    current_24hr = [datetime.fromtimestamp(ts).strftime('%H:%M') for ts in current_time]
    
    #Hourly type information    
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_precipitation_probability = hourly.Variables(2).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(3).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["dew_point_2m"] = hourly_dew_point_2m
    hourly_data["precipitation_probability"] = hourly_precipitation_probability
    hourly_data["weather_code"] = hourly_weather_code
    
    #Daily type information
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_sunrise = daily.Variables(3).ValuesInt64AsNumpy()
    daily_sunset = daily.Variables(4).ValuesInt64AsNumpy()
    daily_precipitation_probability_max = daily.Variables(5).ValuesAsNumpy()
    
    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}
    
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["precipitation_probability_max"] = daily_precipitation_probability_max
    sunrise_24hr = [datetime.fromtimestamp(ts).strftime('%H:%M') for ts in daily_sunrise]
    sunset_24hr = [datetime.fromtimestamp(ts).strftime('%H:%M') for ts in daily_sunset]
    sunrise_12hr = [datetime.fromtimestamp(ts).strftime('%I:%M %p') for ts in daily_sunrise]
    sunset_12hr = [datetime.fromtimestamp(ts).strftime('%I:%M %p') for ts in daily_sunset]
    
    query = ET.Element("query", {
        "xmlns:yahoo": "http://www.yahooapis.com/v1/base.rng",
        "yahoo:count": "2",
        "yahoo:created": "2012-10-30T11:36:42Z",
        "yahoo:lang": "en-US"
    })

    for _ in range(2):
        meta = ET.SubElement(query, "meta")
        weather = ET.SubElement(meta, "weather")
        ET.SubElement(weather, "yahoo_mobile_url").text = "https://openweathermap.org/"
        ET.SubElement(weather, "twc_mobile_url").text = "https://apps.apple.com/us/app/openweather/id1535923697"
        ET.SubElement(weather, "units", {
            "distanceUnits": "km",
            "pressureUnits": "mb",
            "speedUnits": "km/h",
            "tempUnits": "C"
        })

    results = ET.SubElement(query, "results")

    # First location block
    location1 = ET.SubElement(results, "results")
    location = ET.SubElement(location1, "location", {
        "city": str(CityName),
        "country": str(response.Timezone().decode('utf-8')),
        "latitude": str(round(response.Latitude(), 2)),
        "locationID": "ASXX0075",
        "longitude": str(round(response.Longitude(), 2)),
        "state": "",
        "woeid": str(ID)
    })

    currently = ET.SubElement(location, "currently", {
        "barometer": "1005",
        "barometricTrend": "",
        "dewpoint": "",
        "feelsLike": str(round(current.Variables(2).Value(), 2)),
        "heatIndex": str(round(current.Variables(2).Value(), 2)),
        "moonfacevisible": "",
        "moonphase": "",
        "percentHumidity": str(int(current.Variables(1).Value())),
        "sunrise": str(sunrise_12hr[1]),
        "sunrise24": str(sunrise_24hr[1]),
        "sunset": str(sunset_12hr[1]),
        "sunset24": str(sunset_24hr[1]),
        "temp": str(round(current.Variables(2).Value(), 2)),
        "tempBgcolor": "",
        "time": str(current_12hr[0]),
        "time24": str(current_24hr[0]),
        "timezone": str(response.TimezoneAbbreviation().decode('utf-8')),
        "tz": str(response.TimezoneAbbreviation().decode('utf-8')),
        "visibility": "",
        "windChill": str(round(current.Variables(2).Value(), 2)),
        "windDirection": "",
        "windDirectionDegree": str(round(current.Variables(6).Value(), 2)),
        "windSpeed": str(round(current.Variables(5).Value(), 2))
    })
    ET.SubElement(currently, "condition", {"code": str(weather_code_converter(current.Variables(4).Value()))})

    forecast = ET.SubElement(location, "forecast")
    days = [
        {"dayOfWeek": str(get_day_number(daily_data["date"][2])), "poP": "", "high": str(round(daily_temperature_2m_max[1], 2)), "low": str(round(daily_temperature_2m_min[1], 2)), "code": str(weather_code_converter(daily_weather_code[2]))},
        {"dayOfWeek": str(get_day_number(daily_data["date"][3])), "poP": "20", "high": str(round(daily_temperature_2m_max[2], 2)), "low": str(round(daily_temperature_2m_min[2], 2)), "code": str(weather_code_converter(daily_weather_code[3]))},
        {"dayOfWeek": str(get_day_number(daily_data["date"][4])), "poP": "40", "high": str(round(daily_temperature_2m_max[3], 2)), "low": str(round(daily_temperature_2m_min[3], 2)), "code": str(weather_code_converter(daily_weather_code[4]))},
        {"dayOfWeek": str(get_day_number(daily_data["date"][5])), "poP": "60", "high": str(round(daily_temperature_2m_max[4], 2)), "low": str(round(daily_temperature_2m_min[4], 2)), "code": str(weather_code_converter(daily_weather_code[5]))},
        {"dayOfWeek": str(get_day_number(daily_data["date"][6])), "poP": "80", "high": str(round(daily_temperature_2m_max[5], 2)), "low": str(round(daily_temperature_2m_min[5], 2)), "code": str(weather_code_converter(daily_weather_code[6]))},
        {"dayOfWeek": str(get_day_number(daily_data["date"][7])), "poP": "100", "high": str(round(daily_temperature_2m_max[6], 2)), "low": str(round(daily_temperature_2m_min[6], 2)), "code": str(weather_code_converter(daily_weather_code[7]))}
    ]

    for day in days:
        day_elem = ET.SubElement(forecast, "day", {
            "dayOfWeek": day["dayOfWeek"],
            "poP": day["poP"]
        })
        ET.SubElement(day_elem, "temp", {"high": day["high"], "low": day["low"]})
        ET.SubElement(day_elem, "condition", {"code": day["code"]})

    ET.SubElement(forecast, "extended_forecast_url").text = "https://notdbrand.com"

    # Second location block
    location2 = ET.SubElement(results, "results")
    location = ET.SubElement(location2, "location", {"woeid": str(ID)})
    hourlyforecast = ET.SubElement(location, "hourlyforecast")
    
    #Make a for loop for this
    hours = [
        {"time24": str(extract_time(hourly_data["date"][7])), "code": str(weather_code_converter(hourly_weather_code[7])), "poP": str(hourly_pre(hourly_precipitation_probability[7])), "temp": str(round(hourly_temperature_2m[7], 2))},
        {"time24": str(extract_time(hourly_data["date"][8])), "code": str(weather_code_converter(hourly_weather_code[8])), "poP": str(hourly_pre(hourly_precipitation_probability[8])), "temp": str(round(hourly_temperature_2m[8], 2))},
        {"time24": str(extract_time(hourly_data["date"][9])), "code": str(weather_code_converter(hourly_weather_code[9])), "poP": str(hourly_pre(hourly_precipitation_probability[9])), "temp": str(round(hourly_temperature_2m[9], 2))},
        {"time24": str(extract_time(hourly_data["date"][10])), "code": str(weather_code_converter(hourly_weather_code[10])), "poP": str(hourly_pre(hourly_precipitation_probability[10])), "temp": str(round(hourly_temperature_2m[10], 2))},
        {"time24": str(extract_time(hourly_data["date"][11])), "code": str(weather_code_converter(hourly_weather_code[11])), "poP": str(hourly_pre(hourly_precipitation_probability[11])), "temp": str(round(hourly_temperature_2m[11], 2))},
        {"time24": str(extract_time(hourly_data["date"][12])), "code": str(weather_code_converter(hourly_weather_code[12])), "poP": str(hourly_pre(hourly_precipitation_probability[12])), "temp": str(round(hourly_temperature_2m[12], 2))},
        {"time24": str(extract_time(hourly_data["date"][13])), "code": str(weather_code_converter(hourly_weather_code[13])), "poP": str(hourly_pre(hourly_precipitation_probability[13])), "temp": str(round(hourly_temperature_2m[13], 2))},
        {"time24": str(extract_time(hourly_data["date"][14])), "code": str(weather_code_converter(hourly_weather_code[14])), "poP": str(hourly_pre(hourly_precipitation_probability[14])), "temp": str(round(hourly_temperature_2m[14], 2))},
        {"time24": str(extract_time(hourly_data["date"][15])), "code": str(weather_code_converter(hourly_weather_code[15])), "poP": str(hourly_pre(hourly_precipitation_probability[15])), "temp": str(round(hourly_temperature_2m[15], 2))},
        {"time24": str(extract_time(hourly_data["date"][16])), "code": str(weather_code_converter(hourly_weather_code[16])), "poP": str(hourly_pre(hourly_precipitation_probability[16])), "temp": str(round(hourly_temperature_2m[16], 2))},
        {"time24": str(extract_time(hourly_data["date"][17])), "code": str(weather_code_converter(hourly_weather_code[17])), "poP": str(hourly_pre(hourly_precipitation_probability[17])), "temp": str(round(hourly_temperature_2m[17], 2))},
        {"time24": str(extract_time(hourly_data["date"][18])), "code": str(weather_code_converter(hourly_weather_code[18])), "poP": str(hourly_pre(hourly_precipitation_probability[18])), "temp": str(round(hourly_temperature_2m[18], 2))}
    ]

    for hour in hours:
        hour_elem = ET.SubElement(hourlyforecast, "hour", {"time24": hour["time24"]})
        ET.SubElement(hour_elem, "condition", {
            "code": hour["code"],
            "poP": hour["poP"],
            "temp": hour["temp"]
        })


    query_str = ET.tostring(query, encoding='unicode')
    finalR = re.sub(r'\s+(?=<)', '', query_str)

    return finalR

def fetchAndGenerateWeatherXML(lat, lon, woeid, name):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "is_day", "weather_code", "wind_speed_10m", "wind_direction_10m"],
        "hourly": ["temperature_2m", "dew_point_2m", "precipitation_probability", "weather_code", "visibility"],
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "sunrise", "sunset", "precipitation_probability_max"],
        "timezone": "auto",
        "forecast_days": 14
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    xml_return = generateWeatherXML(response, name, woeid)
    print(xml_return)
    return xml_return


def handle_forecast_query(query_param):
    try:
        woeid, lat, lon, cityname = find_coordinates(query_param)
        return fetchAndGenerateWeatherXML(lat, lon, cityname, woeid)
    except ValueError as e:
        print(f"Error: {e}")
        return str(e), 400

@app.route("/v1/yql", methods=["GET"])
def handle_yql():
    query_param = request.args.get("q")
    if not query_param:
        return "Missing query parameter", 400
        
    if "partner.weather.locations" in query_param:
        return handle_search_query(query_param)
    elif "partner.weather.forecasts" in query_param:
        return handle_forecast_query(query_param)
    else:
        return "Unsupported query type", 400

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
