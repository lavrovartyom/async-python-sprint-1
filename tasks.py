import glob
import json
import logging
import multiprocessing
import os
import threading
from pathlib import Path
from typing import Dict, Optional

import requests
from pydantic import ValidationError

from external.analyzer import analyze_json, dump_data
from external.client import YandexWeatherAPI
from logger_config import setup_logging
from models import WeatherData
from utils import CITIES

logger = setup_logging()


class DataFetchingTask:
    def __init__(self, city_name: str, output_path: str | Path) -> None:
        self.city_name: str = city_name
        self.output_path: str | Path = output_path
        self.weather_data: Optional[Dict] = None
        self.thread: threading.Thread = threading.Thread(
            target=self.fetch_and_save_data
        )

    def fetch_and_save_data(self) -> None:
        try:
            logging.info("Starting data fetching for city %s", self.city_name)
            self.weather_data = YandexWeatherAPI.get_forecasting(CITIES[self.city_name])
            if not self.weather_data:
                return
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            with open(self.output_path, "w") as file:
                json.dump(self.weather_data, file, indent=4)
            logging.info(
                "Data for city %s successfully saved in %s",
                self.city_name,
                self.output_path,
            )
        except (OSError, IOError) as e:
            logging.error("Filesystem error for city %s: %s", self.city_name, e)
        except requests.exceptions.RequestException as e:
            logging.error("Network error for city %s: %s", self.city_name, e)

    def start(self) -> None:
        self.thread.start()

    def join(self) -> None:
        self.thread.join()
        return self.weather_data


class DataCalculationTask:
    def __init__(self, input_path: str, output_path: str) -> None:
        self.input_path: str = input_path
        self.output_path: str = output_path
        self.process: multiprocessing.Process = multiprocessing.Process(
            target=self.calculate
        )

    def calculate(self) -> None:
        try:
            logging.info("Starting data analysis for file %s", self.input_path)

            with open(self.input_path, "r") as file:
                data = json.load(file)

            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            analyzed_data = analyze_json(data)
            dump_data(analyzed_data, self.output_path)

            logging.info("Data successfully analyzed and saved in %s", self.output_path)
        except (OSError, IOError) as e:
            logging.error(
                "Filesystem error during processing file %s: %s", self.input_path, e
            )
        except json.JSONDecodeError as e:
            logging.error(
                "JSON decoding error during processing file %s: %s", self.input_path, e
            )
        except Exception as e:
            logging.error(
                "Error during data analysis from file %s: %s", self.input_path, e
            )

    def start(self) -> None:
        self.process.start()

    def join(self) -> None:
        self.process.join()


class DataAggregationTask:
    def __init__(self, input_folder: str, output_file: str) -> None:
        self.input_folder: str = input_folder
        self.output_file: str = output_file
        self.thread: threading.Thread = threading.Thread(target=self.aggregate)

    def aggregate(self) -> None:
        try:
            logging.info("Starting data aggregation")
            aggregated_data = {}
            for file_name in glob.glob(f"{self.input_folder}/*_analysis.json"):
                city_name = os.path.basename(file_name).split("_")[0]
                with open(file_name, "r") as file:
                    data = json.load(file)
                    aggregated_data[city_name] = data

            with open(self.output_file, "w") as file:
                json.dump(aggregated_data, file, indent=4)

            logging.info(
                "Data successfully aggregated and saved in %s", self.output_file
            )
        except (OSError, IOError) as e:
            logging.error("Filesystem error during aggregation: %s", e)
        except json.JSONDecodeError as e:
            logging.error("JSON decoding error during aggregation: %s", e)
        except Exception as e:
            logging.error("Unexpected error during data aggregation: %s", e)

    def start(self) -> None:
        self.thread.start()

    def join(self) -> None:
        self.thread.join()


class DataAnalyzingTask:
    def __init__(self, input_file: str) -> None:
        self.input_file: str = input_file
        self.process: multiprocessing.Process = multiprocessing.Process(
            target=self.analyze
        )

    def analyze(self) -> None:
        try:
            logging.info("Starting final data analysis")
            with open(self.input_file, "r") as file:
                data = json.load(file)
            try:
                weather_data = WeatherData(cities=data)
            except ValidationError as e:
                logging.error("Validation error during data analysis: %s", e)
                return

            best_cities = []
            max_avg_temp = float("-inf")
            max_clear_hours = 0

            for city, city_data in weather_data.cities.items():
                total_temp = 0
                total_clear_hours = 0
                days_count = 0

                for day in city_data.days:
                    if day.temp_avg is not None:
                        total_temp += day.temp_avg
                        days_count += 1
                    total_clear_hours += day.relevant_cond_hours

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

            logging.info("Most favorable cities for travel: %s", ", ".join(best_cities))
        except (OSError, IOError) as e:
            logging.error("Filesystem error during analysis: %s", e)
        except json.JSONDecodeError as e:
            logging.error("JSON decoding error during analysis: %s", e)
        except Exception as e:
            logging.error("Unexpected error during data analysis: %s", e)

    def start(self) -> None:
        self.process.start()

    def join(self) -> None:
        self.process.join()
