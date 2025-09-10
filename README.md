<h1 align="center"> Outlet Sales Weekly Report </h1>

## Stack
![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-1d1717?style=for-the-badge&logo=PostgreSQL&logoColor=fff6f6)
![SQLAlchemy](https://img.shields.io/badge/-SQLAlchemy-1d1717?style=for-the-badge&logo=SQLAlchemy&logoColor=fff6f6)
![Python](https://img.shields.io/badge/-Python-1d1717?style=for-the-badge&logo=Python&logoColor=fff6f6)
![Pandas](https://img.shields.io/badge/-Pandas-1d1717?style=for-the-badge&logo=Pandas&logoColor=fff6f6)
![Matplotlib](https://img.shields.io/badge/-Matplotlib-1d1717?style=for-the-badge&logo=Matplotlib&logoColor=fff6f6)
![Seaborn](https://img.shields.io/badge/-Seaborn-1d1717?style=for-the-badge&logo=Seaborn&logoColor=fff6f6)
![CI/CD](https://img.shields.io/badge/-GitHub%20CI\/CD-1d1717?style=for-the-badge&logo=GitHub&logoColor=fff6f6)

## Description

An automated report that delivers a weekly sales summary for one of our retail outlets every Monday at 9:00 a.m. The report is generated through an ETL pipeline that extracts the required data from a PostgreSQL database, applies transformations, creates visualizations, composes a text message, and sends the result to a Telegram channel using the Telegram API.

**Project Goal** - To provide weekly notifications to decision-makers with key sales figures from the main outlet.

## How it works

Every Monday at 9:00 a.m., decision-makers receive a message that includes a daily revenue chart for the past week.

Example:

<p align="left"> <img src="assets/sales_weekly_report_example.png" width="600"> </p>

The message also contains a summary text describing the main sales figures.

Example:

<p align="left"> <img src="assets/sales_weekly_report_text_example.png" width="350"> </p>