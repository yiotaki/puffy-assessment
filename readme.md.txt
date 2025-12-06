📊 Puffy Analytics Pipeline
A Complete ETL → Journey → Attribution → Revenue Analysis System

This repository contains a modular, production-ready analytics pipeline designed to process multi-source tracking exports from Puffy.com and generate:

Data Quality Reports

Unified ETL Dataset

User Journeys & Funnels

Multi-Channel Attribution (First/Last Touch)

Advanced UTM Analysis

Revenue & Conversion Path Analysis

Monitoring & Anomaly Detection

All functionality is packaged into a single orchestrated script:

puffy_all.py
🚀 Features
1. Automatic CSV Merge + Data Quality QA

Normalizes inconsistent column names

Detects timestamp fields

Computes per-file structural consistency

Detects missing data, duplicates, malformed event_data

Outputs a QA Excel report:

summary

input structure

overall column stats

per-file stats

event-level QA

daily data-quality summary

2. Full ETL Processing

URL parsing + category extraction

UTM parameter extraction

User-Agent parsing (device, OS, browser)

Sessionization using 30-min inactivity window

Deduplication

Revenue extraction from event_data → for checkout_completed

Added revenue columns:

revenue

price

quantity

product

Outputs:

merged_puffy_etl.csv

merged_puffy_etl.parquet

ETL summary + reconciliation reports

3. Journey & Funnel Analysis

Per-user category journeys

Top journeys

Funnel by category

Sankey transition diagrams

Revenue-per-journey summaries

4. Attribution Models

Supports:

UTM First-Touch

UTM Last-Touch

Category-based attribution

Referrer attribution

UTM + Referrer combined attribution

Attribution by device & device family

Attribution revenue metrics

5. Advanced Analysis

Acquisition funnel

UTM funnel for top channels

UTM next-step funnel (7-day lookahead)

Device mix at conversion

Charts for funnel, attribution, device mix

6. Revenue Analysis (Extended)

Revenue overview

Revenue by day

Revenue by UTM channel

Revenue by conversion path

Exported CSV + Excel outputs

7. Monitoring & Anomaly Detection

Daily metrics

Z-score anomaly flags

Daily device mix

Dashboard-ready CSV outputs

🧠 Technology Stack
Component	Description
Python 3.9+	Main pipeline language
pandas	Heavy ETL transformations
numpy	Numerical processing
matplotlib	Charts
openpyxl	Excel outputs
plotly	Sankey diagrams