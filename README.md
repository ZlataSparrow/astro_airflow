Astronomer / Apache Airflow Project
================

This repository contains my local Apache Airflow environment built with the Astronomer CLI. I use this setup to design, develop, and test modern data pipelines. This README explains what the project includes, what I configured, and how anyone can run it locally.

Astro project contains the following files and folders:

- dags/ – Contains all Python files defining Airflow DAGs. This directory includes the default example DAG, example_astronauts, which demonstrates a simple ETL pipeline. It retrieves the current list of astronauts from the Open Notify API and prints a message for each one using the TaskFlow API and dynamic task mapping. For a full walkthrough, see Astronomer’s Getting Started tutorial￼.
- Dockerfile – Defines the Astro Runtime Docker image used to run this Airflow environment. Add any additional system commands, customizations, or runtime overrides here.
- include/ – A place for extra project resources such as SQL files, configuration files, or additional Python modules. Empty by default.
- packages.txt – Use this file to list any system-level packages your project needs. Empty by default.
- requirements.txt – Install any Python packages required by your DAGs or plugins by adding them here. Empty by default.
- plugins/ – The location for custom or community Airflow plugins. Empty by default.
- airflow_settings.yaml – A local configuration file for defining Airflow Connections, Variables, and Pools so you don’t need to manually create them in the UI while developing.

Deploying the Project Locally
===========================

To run Airflow on your local machine, start the environment with: 'astro dev start'

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks


Contact
=======

This environment is based on the Astronomer CLI.
For issues or feedback, see:
https://www.astronomer.io/
