import glob
import logging
from typing import List

from tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
    DataCalculationTask,
    DataFetchingTask,
)
from utils import CITIES


def run_data_fetching_tasks() -> None:
    fetching_tasks: List[DataFetchingTask] = [
        DataFetchingTask(city, f"./data/{city}_weather.json") for city in CITIES
    ]
    for task in fetching_tasks:
        task.start()
    for task in fetching_tasks:
        task.join()


def run_data_calculation_tasks() -> None:
    weather_files: List[str] = glob.glob("./data/*_weather.json")
    calculation_tasks: List[DataCalculationTask] = [
        DataCalculationTask(
            file,
            file.replace("data", "results").replace("_weather.json", "_analysis.json"),
        )
        for file in weather_files
    ]
    for task in calculation_tasks:
        task.start()
    for task in calculation_tasks:
        task.join()


def run_data_aggregation_task() -> None:
    aggregation_task: DataAggregationTask = DataAggregationTask(
        "./results", "./aggregated_data.json"
    )
    aggregation_task.start()
    aggregation_task.join()


def run_data_analyzing_task() -> None:
    analyzing_task: DataAnalyzingTask = DataAnalyzingTask("./aggregated_data.json")
    analyzing_task.start()
    analyzing_task.join()


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    logging.info("Starting data fetching tasks")
    run_data_fetching_tasks()

    logging.info("Starting data calculation tasks")
    run_data_calculation_tasks()

    logging.info("Starting data aggregation task")
    run_data_aggregation_task()

    logging.info("Starting data analyzing task")
    run_data_analyzing_task()


if __name__ == "__main__":
    main()
