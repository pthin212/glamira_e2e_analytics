## Overview
Addressing fragmented business data, this project **automates ELT processes** and **constructs a centralized Data Warehouse**.  It further **provides flexible analytics tools** and implements **data visualization via dashboards** to streamline data processing, enhance analysis quality, and **support data-driven business decisions**.
Addressing fragmented business data, this project automates ELT processes, constructs a centralized Data Warehouse, provides flexible analytics tools, and implements data visualization via dashboards to streamline data processing, enhance analysis quality, and support data-driven business decisions.
## 1. Data Pipeline Architecture
A cloud-native ***ELT*** pipeline architecture integrates ***Data Source*** ingestion, ***Data Lake*** (GCS) raw data storage, ***Data Warehouse*** transformation via ***dbt*** for analytical modeling, and ***Looker Studio*** dashboards for actionable business intelligence.

![dddddd](img/elt_data_pipeline.png)
## 2. Data Lineage
This section illustrates ***how data flows through dbt models***, providing transparency and traceability ***from raw sources to final outputs***.

![](img/data_lineage.png)

