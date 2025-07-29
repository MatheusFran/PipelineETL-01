import requests

def ExtractData():
    url = 'https://api.football-data.org/v4/competitions'
    response = requests.get(url)
    return response.json()