import sqlite3
import pandas as pd
import schedule
import time
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import requests

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
    api_key = ''
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

def plot_all_data():
    conn = sqlite3.connect('weather_data.db')
    df = pd.read_sql_query("SELECT * FROM weather", conn)
    conn.close()

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    fig, axs = plt.subplots(2, 2, figsize=(20, 15))

    # Line plot for temperature and humidity over time
    sns.lineplot(ax=axs[0, 0], x=df['timestamp'], y=df['temperature'], label='Temperature')
    sns.lineplot(ax=axs[0, 0], x=df['timestamp'], y=df['humidity'], label='Humidity')
    axs[0, 0].set_xlabel('Time')
    axs[0, 0].set_ylabel('Value')
    axs[0, 0].set_title('Weather Trends')
    axs[0, 0].legend()

    # Scatter plot for temperature vs. humidity
    sns.scatterplot(ax=axs[0, 1], x='temperature', y='humidity', data=df)
    axs[0, 1].set_xlabel('Temperature (°C)')
    axs[0, 1].set_ylabel('Humidity (%)')
    axs[0, 1].set_title('Temperature vs Humidity Scatter Plot')

    # Histogram for temperature distribution
    sns.histplot(ax=axs[1, 0], x=df['temperature'], bins=20, kde=True)
    axs[1, 0].set_xlabel('Temperature (°C)')
    axs[1, 0].set_ylabel('Frequency')
    axs[1, 0].set_title('Temperature Distribution')

    # Pie chart for humidity distribution
    conditions = [
        (df['humidity'] < 30),
        (df['humidity'] >= 30) & (df['humidity'] <= 60),
        (df['humidity'] > 60)
    ]
    choices = ['Low Humidity', 'Medium Humidity', 'High Humidity']
    df['humidity_category'] = pd.cut(df['humidity'], bins=[-1, 30, 60, 100], labels=choices)

    humidity_counts = df['humidity_category'].value_counts().reset_index()
    humidity_counts.columns = ['Category', 'Count']

    axs[1, 1].pie(humidity_counts['Count'], labels=humidity_counts['Category'], autopct='%1.1f%%', startangle=140)
    axs[1, 1].set_title('Humidity Distribution')

    plt.tight_layout()
    plt.show()

def daily_analysis():
    print("Running Analysis")
    avg_temp, avg_humidity = analyze_data()
    print(f"Average Temperature: {avg_temp}°C")  # Debug print
    print(f"Average Humidity: {avg_humidity}%")  # Debug print
    plot_all_data()

if __name__ == "__main__":
    create_db()
    job()
    daily_analysis()
    schedule.every(2).minutes.do(job)
    schedule.every().day.at("00:00").do(daily_analysis)

    while True:
        schedule.run_pending()
        time.sleep(1)
