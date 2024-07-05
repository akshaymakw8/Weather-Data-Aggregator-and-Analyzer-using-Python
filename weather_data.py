import sqlite3
import pandas as pd
import schedule
import time
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import requests
import plotly.express as px
import webbrowser
import os

def create_db():
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS weather
                 (timestamp TEXT, location TEXT, temperature REAL, humidity REAL, wind_speed REAL)''')
    conn.commit()
    conn.close()

def fetch_weather_data(api_key, location):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={location}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        print("API Response:", response.json())  # Debug print
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def store_weather_data(data, location):
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()

    timestamp = datetime.utcfromtimestamp(data['dt']).strftime('%Y-%m-%d %H:%M:%S')
    c.execute("INSERT INTO weather (timestamp, location, temperature, humidity, wind_speed) VALUES (?, ?, ?, ?, ?)",
              (timestamp, location, data['main']['temp'], data['main']['humidity'], data['wind']['speed']))
    conn.commit()
    print("Data Stored")
    conn.close()

def job():
    print("Starting Job")
    api_key = '4f0c83da004158f2c20cff9f92995446'
    location = 'Pune, IN'  # Replace with your desired location
    data = fetch_weather_data(api_key, location)
    if data:
        store_weather_data(data, location)
        print(f"Fetched data successfully at {datetime.now()}")  # Debug print
    else:
        print("Failed to fetch data")  # Debug print

def analyze_data():
    conn = sqlite3.connect('weather_data.db')
    df = pd.read_sql_query("SELECT * FROM weather", conn)
    conn.close()

    avg_temp = df['temperature'].mean()
    avg_humidity = df['humidity'].mean()

    return avg_temp, avg_humidity

def plot_data():
    conn = sqlite3.connect('weather_data.db')
    df = pd.read_sql_query("SELECT * FROM weather", conn)
    conn.close()

    plt.figure(figsize=(14, 7))
    sns.lineplot(x=pd.to_datetime(df['timestamp']), y=df['temperature'], label='Temperature')
    sns.lineplot(x=pd.to_datetime(df['timestamp']), y=df['humidity'], label='Humidity')
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.title('Weather Trends')
    plt.legend()
    plt.show()

def plot_scatter():
    conn = sqlite3.connect('weather_data.db')
    df = pd.read_sql_query("SELECT * FROM weather", conn)
    conn.close()

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    fig = px.scatter(df, x='temperature', y='humidity', color='timestamp',
                     title='Temperature vs Humidity Scatter Plot',
                     labels={'temperature': 'Temperature (°C)', 'humidity': 'Humidity (%)'},
                     hover_data=['timestamp'])

    html_file = 'scatter_plot.html'
    fig.write_html(html_file)
    webbrowser.open('file://' + os.path.realpath(html_file))

def plot_histogram():
    conn = sqlite3.connect('weather_data.db')
    df = pd.read_sql_query("SELECT * FROM weather", conn)
    conn.close()

    plt.figure(figsize=(14, 7))
    sns.histplot(df['temperature'], bins=20, kde=True)
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Frequency')
    plt.title('Temperature Distribution')
    plt.show()

def plot_pie_chart():
    conn = sqlite3.connect('weather_data.db')
    df = pd.read_sql_query("SELECT * FROM weather", conn)
    conn.close()

    conditions = [
        (df['humidity'] < 30),
        (df['humidity'] >= 30) & (df['humidity'] <= 60),
        (df['humidity'] > 60)
    ]
    choices = ['Low Humidity', 'Medium Humidity', 'High Humidity']
    df['humidity_category'] = pd.cut(df['humidity'], bins=[-1, 30, 60, 100], labels=choices)

    humidity_counts = df['humidity_category'].value_counts().reset_index()
    humidity_counts.columns = ['Category', 'Count']

    fig = px.pie(humidity_counts, names='Category', values='Count', title='Humidity Distribution')

    html_file = 'pie_chart.html'
    fig.write_html(html_file)
    webbrowser.open('file://' + os.path.realpath(html_file))

def daily_analysis():
    print("Running Analysis")
    avg_temp, avg_humidity = analyze_data()
    print(f"Average Temperature: {avg_temp}°C")  # Debug print
    print(f"Average Humidity: {avg_humidity}%")  # Debug print
    plot_data()
    plot_scatter()
    plot_histogram()
    plot_pie_chart()

if __name__ == "__main__":
    create_db()
    job()
    daily_analysis()
    schedule.every(2).minutes.do(job)
    schedule.every().day.at("00:00").do(daily_analysis)

    while True:
        schedule.run_pending()
        time.sleep(1)
