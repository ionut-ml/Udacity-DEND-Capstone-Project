# US Immigration Data Lake Pipeline using Spark and AWS
### Data Engineering Capstone Project

#### Project Summary

This project uses datasets on US Immigration, Demographics and Global Temperatures. The goal of the project is to combine the skills learned throught the Udacity Data Engineering Nano-degree and prepare an ETL pipeline and a Data Model to be used for analytics.
We do this by gathering and exploring source data in order to determine a proper Data Model that suits our purposes. Then we build a data pipeline that extracts the source information, transforms it by various clean-up steps, the loads it into the final tables of our defined data model.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Scope the Project and Gather Data

#### Scope

In this project we plan to use the datasets on US Immigration, Demographics and Global Temperatures as source files, develop a data pipeline in order to prepare them for loading into a data lake stored as parquet files on AWS S3 using a Data Model that we will define along the way.

With this scope in hand, the main phases of the development will be the following:
1. **Load** the datasets and do some **Exploratory Data Analysis**.
    
    This will do two things: first, allows us to understand the data, which will build our intuition for what the final Data Model will be, and secondly, it will start revealing the necessary steps in our Data Pipeline (e.g.: how to load each dataset, what clean-up steps are needed, what flaws we find, what data quality checks might be needed, etc.)


2. Perform **clean-up steps** on each of the datasets.
    
    With the knowledge gathered in the previous phase, we start actually implementing cleaning steps and check further each datasets for potential quality issues. This will be the basis for some functions that will be used in our Data Pipeline.
    

3. **Define our Data Model**.
    
    In this phase, we will think more closely about how the final Data Model will look like, what will be the goal for it, what will be our fact and dimension tables, what schema will we use.
    As we will see below, it will be shown that a slightly modified variant of the classic star schema will best suit our needs of providing a simple, easy-to-understand model that also allows for efficient queries for analytical purposes.
    

4. Build the **Data Pipeline** and perform **Data Quality Checks**.
    
    In this phase, we use the knowledge and steps identified earlier in order to build functions that will extract data from the source files, transform it by various clean-up steps, then load it into our fact and dimension tables previously defined.
    Finally, we perform some simple data quality checks in order to ensure that our data pipeline ran as expected.

To this end the technologies that we used are: python, Spark, AWS, Jupyter Notebooks.

#### Describe the Datasets

##### 1. I94 Immigration Data

This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. There's a sample file so you can take a look at the data in csv format before reading it all in. You do not have to use the entire dataset, just use what you need to accomplish the goal you set at the beginning of the project.

Data Dictionary:

* **cicid**: Unique record ID
* **i94yr**: 4 digit year
* **i94mon**: Numeric month
* **i94cit**: 3 digit code for immigrant country of birth
* **i94res**: 3 digit code for immigrant country of residence
* **i94port**: Port of admission
* **arrdate**: Arrival Date in the USA
* **i94mode**: Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)
* **i94addr**: USA State of arrival
* **depdate**: Departure Date from the USA
* **i94bir**: Age of Respondent in Years
* **i94visa**: Visa codes collapsed into three categories
* **count**: Field used for summary statistics
* **dtadfile**: Character Date Field - Date added to I-94 Files
* **visapost**: Department of State where where Visa was issued
* **occup**: Occupation that will be performed in U.S
* **entdepa**: Arrival Flag - admitted or paroled into the U.S.
* **entdepd**: Departure Flag - Departed, lost I-94 or is deceased
* **entdepu**: Update Flag - Either apprehended, overstayed, adjusted to perm residence
* **matflag**: Match flag - Match of arrival and departure records
* **biryear**: 4 digit year of birth
* **dtaddto**: Character Date Field - Date to which admitted to U.S. (allowed to stay until)
* **gender**: Non-immigrant sex
* **insnum**: INS number
* **airline**: Airline used to arrive in U.S.
* **admnum**: Admission Number
* **fltno**: Flight number of Airline used to arrive in U.S.
* **visatype**: lass of admission legally admitting the non-immigrant to temporarily stay in U.S.

##### 2. Temperature Data 

This dataset came from Kaggle. From their dataset description:
*We have repackaged the data from a newer compilation put together by the [Berkeley Earth](http://berkeleyearth.org/about/), which is affiliated with Lawrence Berkeley National Laboratory. The Berkeley Earth Surface Temperature Study combines 1.6 billion temperature reports from 16 pre-existing archives. It is nicely packaged and allows for slicing into interesting subsets (for example by country). They publish the source data and the code for the transformations they applied. They also use methods that allow weather observations from shorter time series to be included, meaning fewer observations need to be thrown away.*

The raw data comes from the [Berkeley Earth data page](http://berkeleyearth.org/data/).

Data Dictionary:

* **dt**: Date
* **AverageTemperature**: Global average land temperature in celsius
* **AverageTemperatureUncertainty**: 95% confidence interval around the average
* **City**: Name of City
* **Country**: Name of Country
* **Latitude**: City Latitude
* **Longitude**: City Longitude

##### 3. U.S. City Demographic Data

This data comes from OpenSoft.

This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 

This data comes from the US Census Bureau's 2015 American Community Survey.

Reference: https://www.census.gov/data/developers/about/terms-of-service.html

Data Dictionary:

* **City**: City Name
* **State**: US State where city is located
* **Median Age**: Median age of the population
* **Male Population**: Count of male population
* **Female Population**: Count of female population
* **Total Population**: Count of total population
* **Number of Veterans**: Count of total Veterans
* **Foreign born**: Count of residents of the city that were not born in the city
* **Average Household Size**: Average city household size
* **State Code**: Code of the US state
* **Race**: Respondent race
* **Count**: Count of city's individual per race

### Define the Data Model and Data Pipeline

#### Conceptual Data Model

In order to optimize for simple, easy-to-understand, efficient, queries for analytical use of the Database, the star schema is the best solution for this scope. We also applied a small change to the classic star schema to include the Temperatures data with a relationship to the US Cities.

![Alt text](./Udacity-DEND-Capstone-ERD.svg)

#### Mapping Out Data Pipelines

The pipeline has the following steps:
1. Extract the data from the .CSV or parquet source files.
2. Transform, or clean the data by dropping duplicates, nulls and, optionally, columns with mostly or only nulls.
3. Create the 4 dimension tables from the I94 Immigration, USA Demographics & Temparatures datasets by selecting required column, adjusting names and data types, and extracting other information (e.g.: data/time formats).
4. Create the i94 immigration fact table.

#### Data Quality Checks

The data quality checks performed on the final datasets in order to ensure that the pipeline ran correctly and as expected are the following:
* Check total number of rows in dataset. If no rows are loaded, it means that the pipeline failed.
* Check number of duplicates. If it is greater than 0, it means that the data clean-up did not work correctly and we have duplicate data in our tables.

#### Data dictionary

**Dimension Tables**:

1. **dim_immigrant**:
    * ccid: Unique record ID
    * i94cit: 3 digit code for immigrant country of birth
    * i94res: 3 digit code for immigrant country of residence
    * i94mode: Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)
    * depdate: Departure Date from the USA
    * i94bir: Age of Respondent in Years
    * gender: Non-immigrant sex
2. **dim_usa_demographics**:
    * id: Unique record ID
    * city: City Name
    * state: US State where city is located
    * median_age: Median age of the population
    * male_pop: Count of male population
    * female_pop: Count of female population
    * total_pop: Count of total population
    * veteran_number: Count of total Veterans
    * foreign_born: Count of residents of the city that were not born in the city
    * avg_household_size: Average city household size
    * race: Respondent race
    * count: Count of city's individual per race
3. **dim_temperatures**:
    * dt: Date
    * avg_temp: Global average land temperature in celsius
    * avg_temp_uncertainty: 95% confidence interval around the average
    * city: Name of City
    * country: Name of Country
    * latitude: City Latitude
    * longitude: City Longitude
4. **dim_calendar**:
    * arrdate: Arrival Date in the USA
    * year: 4 digit year
    * month: Numeric month of year
    * day: Numeric day of month
    * week: Numeric week of year
    * weekday: Day of week.

**Fact Table**:
1. **fact_i94_immigration**:
    * ccid: Unique record ID
    * arrdate: Arrival Date in the USA
    * count: Field used for summary statistics
    * visapost: Department of State where where Visa was issued
    * entdepa: Arrival Flag - admitted or paroled into the U.S.
    * entdepd: Departure Flag - Departed, lost I-94 or is deceased
    * biryear: 4 digit year of birth
    * dtaddto: 	Character Date Field - Date to which admitted to U.S. (allowed to stay until)
    * airline: 	Airline used to arrive in U.S.
    * fltno: Flight number of Airline used to arrive in U.S.
    * visatype: Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
    * i94addr: USA State of arrival
    * i94visa: Visa codes collapsed into three categories
    
#### Complete Project Write Up
* *What is target audience, who is going to utilize the final data model?*
    
    The final data lake will be best suited for an analytics team with familiarity of the technologies used and the skills to connect, analyze and even visualize the resulting insights. 
    
    
* *What are some of the types of questions we can handle using the proposed data model?, Please be specific as we also need to show the output in the notebook at the end to validate if the data model is able to answer the questions*
    
    This Data Model will allow us to answer questions such as: 
        - What are the 10 US states with the most immigrants arriving?
        - What are the 10 countries of birth with the most immigrants to the US?
        - What is the gender distribution? 
        - What is the average age?



* *Clearly state the rationale for the choice of tools and technologies for the project.*
    
    The reason for using Apache Spark is its scalability, and with its pyspark implementation the ease-of-use and easy data manipulation capabilities. It allows us to build a pipeline using initial smaller datasets, and it will be perfectly capable to scale and perform efficiently when the data increases.
    Jupyter Notebooks are used for interactivity and fast data exploration and in order to build the initial version of the pipeline.
    AWS, specifically S3, is one of the best solution on the market for storage, and as with Spark, it's easily scalable. We will use S3 to store our final data lake, in parquet files.
    
    
* *Propose how often the data should be updated and why.*
    
    The main factor that determines the data refresh rate is how often do the source datasets are updated. Since the Temperatures and I94 Immigration datasets are updated monthly, this should be what we must aim for.
    
    
* *Write a description of how you would approach the problem differently under the following scenarios:*
 * *The data was increased by 100x.* 
     We could use a cluster manager like Yarn or spin-up an EMR cluster that can scale our compute resourses. Spark can then handle technology-wise the scale of the data. Also, on the storage front, S3 can scale up or down according to the data lake size.
     
 * *The data populates a dashboard that must be updated on a daily basis by 7am every day.*
     The ETL process would be handled and scheduled by a tool like Apache Airflow. This will allows us to set-up the refresh times on a daily basis.  
 * *The database needed to be accessed by 100+ people.*
     We can increase our nodes using Yarn or EMR, or, alternatively, move our data lake to a cloud, managed Database like Redshfit that is designed for analytical purposes, has very fast read times, and can handle high workloads (users performing queries on the DB).