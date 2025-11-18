"""
## Astronaut ETL example DAG V2

This is an improved version of the example_astronauts DAG with the following enhancements:

1. **Better Error Handling**: Specific exception handling with proper logging
2. **Configuration Management**: Uses Airflow Variables for configurable parameters
3. **Data Persistence**: Saves results to a JSON file
4. **Data Validation**: Validates API response structure
5. **Data Aggregation**: Adds a summary task to aggregate results
6. **Proper Logging**: Uses Python logging instead of print statements
7. **Task Timeouts**: Configurable timeouts for tasks
8. **Better Type Hints**: Improved type annotations
9. **Data Transformation**: Processes and structures data before saving

This DAG queries the list of astronauts currently in space from the
Open Notify API, validates the data, processes it, and saves the results.
"""

import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import Asset, dag, task
from airflow.models import Variable
from pendulum import datetime
import requests
from requests.exceptions import RequestException, Timeout, HTTPError

# Configure logging
logger = logging.getLogger(__name__)

# Default configuration values (can be overridden by Airflow Variables)
DEFAULT_API_URL = "http://api.open-notify.org/astros.json"
DEFAULT_API_TIMEOUT = 10
DEFAULT_GREETING = "Hello! :)"
DEFAULT_OUTPUT_PATH = "/tmp/astronauts_data.json"


@dag(
    dag_id="example_astronauts_v2",
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example", "v2", "improved"],
    catchup=False,
)
def example_astronauts_v2():
    """
    Enhanced version of the astronauts DAG with improved error handling,
    data persistence, and better structure.
    """

    @task(
        outlets=[Asset("current_astronauts")],
        retries=2,
        retry_delay=timedelta(minutes=1),
        task_id="fetch_astronauts_data",
    )
    def get_astronauts(**context) -> dict[str, Any]:
        """
        Fetches astronaut data from the Open Notify API with proper error handling
        and validation. Returns a structured dictionary with metadata and astronaut list.
        """
        # Get configuration from Airflow Variables or use defaults
        api_url = Variable.get("ASTRONAUTS_API_URL", default_var=DEFAULT_API_URL)
        api_timeout = int(
            Variable.get("ASTRONAUTS_API_TIMEOUT", default_var=DEFAULT_API_TIMEOUT)
        )

        logger.info(f"Fetching astronaut data from {api_url}")

        try:
            response = requests.get(api_url, timeout=api_timeout)
            response.raise_for_status()

            data = response.json()

            # Validate response structure
            if "number" not in data or "people" not in data:
                raise ValueError(
                    "Invalid API response structure: missing 'number' or 'people' fields"
                )

            if not isinstance(data["people"], list):
                raise ValueError("Invalid API response: 'people' must be a list")

            number_of_people = data["number"]
            list_of_people = data["people"]

            logger.info(f"Successfully fetched data for {number_of_people} astronauts")

            result = {
                "number": number_of_people,
                "people": list_of_people,
                "fetch_timestamp": context["data_interval_end"].isoformat(),
                "source": "api",
            }

            # Push metadata to XCom for downstream tasks
            context["ti"].xcom_push(
                key="number_of_people_in_space", value=number_of_people
            )
            context["ti"].xcom_push(
                key="fetch_timestamp", value=result["fetch_timestamp"]
            )

            return result

        except Timeout:
            logger.error(f"Request to {api_url} timed out after {api_timeout} seconds")
            raise
        except HTTPError as e:
            logger.error(f"HTTP error occurred: {e.response.status_code} - {e}")
            raise
        except RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except (ValueError, KeyError) as e:
            logger.error(f"Data validation error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            raise

    @task(
        retries=1,
        task_id="process_astronaut_data",
    )
    def process_astronaut_data(astronaut_data: dict[str, Any]) -> list[dict[str, str]]:
        """
        Processes and transforms astronaut data into a structured format.
        Groups astronauts by craft and adds processed metadata.
        """
        logger.info("Processing astronaut data")

        processed_astronauts = []
        craft_counts = {}

        for person in astronaut_data["people"]:
            craft = person.get("craft", "Unknown")
            name = person.get("name", "Unknown")

            # Count astronauts per craft
            craft_counts[craft] = craft_counts.get(craft, 0) + 1

            processed_astronaut = {
                "name": name,
                "craft": craft,
                "processed": True,
            }
            processed_astronauts.append(processed_astronaut)

        logger.info(f"Processed {len(processed_astronauts)} astronauts")
        logger.info(f"Craft distribution: {craft_counts}")

        return processed_astronauts

    @task(
        retries=1,
        task_id="print_astronaut_info",
    )
    def print_astronaut_info(
        greeting: str, person_in_space: dict[str, str]
    ) -> dict[str, str]:
        """
        Prints information about an astronaut and returns a formatted message.
        Uses proper logging instead of print statements.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        message = f"{name} is currently in space flying on the {craft}! {greeting}"
        logger.info(message)

        return {
            "name": name,
            "craft": craft,
            "message": message,
        }

    @task(
        retries=1,
        task_id="create_summary",
    )
    def create_summary(
        astronaut_data: dict[str, Any],
        processed_astronauts: list[dict[str, str]],
    ) -> dict[str, Any]:
        """
        Creates a summary of the astronaut data including statistics and metadata.
        """
        logger.info("Creating summary")

        # Count astronauts by craft
        craft_distribution = {}
        for person in processed_astronauts:
            craft = person["craft"]
            craft_distribution[craft] = craft_distribution.get(craft, 0) + 1

        summary = {
            "total_astronauts": astronaut_data["number"],
            "craft_distribution": craft_distribution,
            "fetch_timestamp": astronaut_data["fetch_timestamp"],
            "source": astronaut_data["source"],
        }

        logger.info(f"Summary created: {summary}")
        return summary

    @task(
        retries=1,
        task_id="save_results",
    )
    def save_results(
        astronaut_data: dict[str, Any],
        processed_astronauts: list[dict[str, str]],
        summary: dict[str, Any],
        **context,
    ) -> str:
        """
        Saves all results to a JSON file for persistence.
        """
        output_path = Variable.get(
            "ASTRONAUTS_OUTPUT_PATH", default_var=DEFAULT_OUTPUT_PATH
        )

        logger.info(f"Saving results to {output_path}")

        output_data = {
            "raw_data": astronaut_data,
            "processed_astronauts": processed_astronauts,
            "summary": summary,
            "dag_run_id": context["dag_run"].run_id,
        }

        # Create directory if it doesn't exist
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Write to file
        with open(output_file, "w") as f:
            json.dump(output_data, f, indent=2)

        logger.info(f"Results saved successfully to {output_path}")
        return output_path

    # Task flow
    raw_data = get_astronauts()
    processed_astronauts = process_astronaut_data(raw_data)

    # Get greeting from Airflow Variable or use default
    greeting = Variable.get("ASTRONAUTS_GREETING", default_var=DEFAULT_GREETING)

    # Use dynamic task mapping to process each astronaut
    astronaut_info = print_astronaut_info.partial(greeting=greeting).expand(
        person_in_space=processed_astronauts
    )

    # Create summary
    summary = create_summary(raw_data, processed_astronauts)

    # Save all results
    save_results(raw_data, processed_astronauts, summary)


# Instantiate the DAG
example_astronauts_v2()

