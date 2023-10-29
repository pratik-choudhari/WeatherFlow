import os
import random
import logging
import requests

from airflow.models import Variable

def fetch_api_key():
    print("hello")
    print(Variable.get("API_KEY1"))
    keys = [Variable.get("API_KEY1"), Variable.get("API_KEY2")]

    if not keys:
        raise EnvironmentError("No API keys found")
    
    return random.choice(keys)


def call_api(api_key: str) -> dict:
    try:
        resp = requests.get("https://api.openweathermap.org/data/2.5/weather?q=Windsor,CA" \
                            f"&appid={api_key}&units=metric")
    except requests.exceptions as e:
        logging.exception("Failed polling api")
        return {}
    
    resp = resp.json()

    data = {"base": resp["base"], "temp": resp["main"]["temp"], "feels_like": resp["main"]["feels_like"], 
            "pressure": resp["main"]["pressure"], "humidity": resp["main"]["humidity"], "visibility": resp["visibility"], 
            "cloudiness": resp["clouds"]["all"], "timestamp": resp["dt"], "sunrise_ts": resp["sys"]["sunrise"], 
            "sunset_ts": resp["sys"]["sunset"], "timezone": resp["timezone"]}
    return data
