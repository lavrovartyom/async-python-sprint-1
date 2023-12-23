from pathlib import Path

from external.client import YandexWeatherAPI
from utils import CITIES
import threading
from logger_config import setup_logging
import glob
import json
import os
import logging
import multiprocessing
from external.analyzer import analyze_json, dump_data


logger = setup_logging()


class DataFetchingTask:
    def __init__(self, city_name: str, output_path: str | Path):
        self.city_name = city_name
        self.output_path = output_path
        self.weather_data = None
        self.thread = threading.Thread(target=self.fetch_and_save_data)

    def fetch_and_save_data(self):
        try:
            logging.info(f"Начинаем получение данных для города {self.city_name}")
            self.weather_data = YandexWeatherAPI.get_forecasting(CITIES[self.city_name])
            if not self.weather_data:
                return
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            with open(self.output_path, "w") as file:
                json.dump(self.weather_data, file, indent=4)
            logging.info(
                f"Данные для города {self.city_name} успешно сохранены в {self.output_path}"
            )
        except Exception as e:
            logging.error(
                f"Ошибка при получении данных для города {self.city_name}: {e}"
            )

    def start(self):
        self.thread.start()

    def join(self):
        self.thread.join()
        return self.weather_data


class DataCalculationTask:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.process = multiprocessing.Process(target=self.calculate)

    def calculate(self):
        try:
            logging.info(f"Начинаем анализ данных для файла {self.input_path}")
            with open(self.input_path, "r") as file:
                data = json.load(file)
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            analyzed_data = analyze_json(data)
            dump_data(analyzed_data, self.output_path)
            logging.info(
                f"Данные успешно проанализированы и сохранены в {self.output_path}"
            )
        except Exception as e:
            logging.error(f"Ошибка при анализе данных из файла {self.input_path}: {e}")

    def start(self):
        self.process.start()

    def join(self):
        self.process.join()


class DataAggregationTask:
    def __init__(self, input_folder, output_file):
        self.input_folder = input_folder
        self.output_file = output_file
        self.thread = threading.Thread(target=self.aggregate)

    def aggregate(self):
        try:
            logging.info("Начинаем агрегацию данных")
            aggregated_data = {}
            for file_name in glob.glob(f"{self.input_folder}/*_analysis.json"):
                city_name = os.path.basename(file_name).split("_")[0]
                with open(file_name, "r") as file:
                    data = json.load(file)
                    aggregated_data[city_name] = data

            with open(self.output_file, "w") as file:
                json.dump(aggregated_data, file, indent=4)

            logging.info(
                f"Данные успешно агрегированы и сохранены в {self.output_file}"
            )
        except Exception as e:
            logging.error(f"Ошибка при агрегации данных: {e}")

    def start(self):
        self.thread.start()

    def join(self):
        self.thread.join()


class DataAnalyzingTask:
    def __init__(self, input_file):
        self.input_file = input_file
        self.process = multiprocessing.Process(target=self.analyze)

    def analyze(self):
        try:
            logging.info("Начинаем финальный анализ данных")
            with open(self.input_file, "r") as file:
                data = json.load(file)

            best_cities = []
            max_avg_temp = float("-inf")
            max_clear_hours = 0

            for city, city_data in data.items():
                if "days" not in city_data:
                    logging.warning(f"Отсутствуют данные 'days' для города {city}")
                    continue

                total_temp = 0
                total_clear_hours = 0
                days_count = 0

                for day in city_data["days"]:
                    if day["temp_avg"] is not None:
                        total_temp += day["temp_avg"]
                        days_count += 1
                    total_clear_hours += day["relevant_cond_hours"]

                if days_count > 0:
                    avg_temp = total_temp / days_count
                    if avg_temp > max_avg_temp or (
                        avg_temp == max_avg_temp and total_clear_hours > max_clear_hours
                    ):
                        best_cities = [city]
                        max_avg_temp = avg_temp
                        max_clear_hours = total_clear_hours
                    elif (
                        avg_temp == max_avg_temp
                        and total_clear_hours == max_clear_hours
                    ):
                        best_cities.append(city)

            logging.info(
                f"Наиболее благоприятные города для поездки: {', '.join(best_cities)}"
            )
        except Exception as e:
            logging.error(f"Ошибка при анализе данных: {e}")

    def start(self):
        self.process.start()

    def join(self):
        self.process.join()
