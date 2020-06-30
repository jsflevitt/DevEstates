# DevEstates

A database for visualizing and understanding the impacts of federal disasters on real estate prices.

[Presentation Link](https://docs.google.com/presentation/d/1fM4c-hD97hTDSLZJ_2v7Q0EnnE1rqgqjD7lkrD-M9ck/edit?usp=sharing).

<hr/>

## Introduction
Large disasters are known to depress real estate prices for months or years. Some areas recover quickly while others do not. This database is designed to assist home buyers and sellers, investors, and researchers in understanding past temporospatial trends in real estate pricing while enabling them to make predictions regarding current and future events.

## Architecture
An outline of the data pipeline architecture. Not every database requires geospark and utilize Apache Spark. The ETL cluster can be reconfigured to optimize the processing of the incoming data. Please see [EC2 Setups](https://github.com/jsflevitt/DevEstates/tree/master/Setup) for scripts and more details.

![Data Pipeline](https://github.com/jsflevitt/DevEstates/blob/master/images/DataPipelineOverview.png)

## Datasets

### Real Estate Data Sources:
These consistute a small sample of what is available.

#### Private
- [Zillow](https://www.quandl.com/data/ZILLOW-Zillow-Real-Estate-Research)
- [Realtor.com](https://www.realtor.com/research/data/)

#### Public
- [NYC Open Data](https://data.cityofnewyork.us/browse?q=DOF%3A%20Neighborhood%20Sales%20by%20Neighborhood%20Citywide%20by%20Borough&sortBy=relevance)
- [Maryland Open Data](https://opendata.maryland.gov/Business-and-Economy/Maryland-Real-Property-Assessments-Hidden-Property/ed4q-f8tm)

### Disaster Data Sources:
These Federal Databases contain both geospatial and impact data, but frequently provide less information on exact dates.
#### Fire Perimeter Data
- [National Interagency Fire Center](https://data-nifc.opendata.arcgis.com)
- [Monitoring Trends in Burn Severity](https://www.mtbs.gov/direct-download)

#### General Hazard Data
- [open FEMA](https://www.fema.gov/data-sets)

## Engineering challenges
Challenges in creating this database came in two main areas:
1. Working with the myriad different databases that store real estate sales and listing prices
      1. Creating a unified data model to connect the variety of data inputs
      2. Deploying customized ETL clusters to better handle varied data input sources
2. Creating a database that works efficiently with data organized in both time and space
      1. A significant amount of preprocessing is done with geospark in order to speed up later database calls
      2. The database is deployed using both PostGIS and TimescaleDB to facilitate temporospatial indexing and deployment
