# Emissions Displays

This current project involves building an ETL pipeline from the EPA streaming emissions website. Per the website, the data is released quarterly, with the script pulling data from the most recent month in the quarter available.
Prefect is used to schedule the deployment of the script once every quarter. The purpose of this project is to build relevant dahsboards in Power BI that update as the data does. Data was sent to postgres to accomplish this.
The dashboard will update with the most recent month in the quarter per the prefect deployment. The timeframes of the project will increase, with further analyses built when the data is stored on a cloud service. 

## Data Cleaning/Manipulation

The data was configured within power BI based on EPA recommendations and the needs of the display. For example, gross load data was multiplied by operating time, which can be summed in hourly intervals using slicer selections. Emission rates were calculated by dividing output mass by heat input mass per the reporting time period. When using slicer options to review emissions data per fuel type, refer to the EPA website for limitations on these views. The API request limits incoming data only when units are operating, as emissions data is not reported otherwise.  


SOURCE: United States Environmental Protection Agency (EPA). “Clean Air Markets Program Data.” Washington, DC: Office of Atmospheric Protection, Clean Air and Power Division. Available from EPA’s Air Markets Program Data web site: https://campd.epa.gov/.
