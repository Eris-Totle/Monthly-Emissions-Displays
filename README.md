# Emissions Displays

This current project involves building an ETL pipeline from the EPA streaming emissions website. Per the website, the data is released quarterly, with the script pulling data from the most recent month in the quarter available.
Prefect is used to schedule the deployment of the script once every quarter. The purpose of this project is to build relevant dahsboards in Power BI that update as the data does. Data was sent to postgres to accomplish this.
The dashboard will update with the most recent month in the quarter per the prefect deployment. The timeframes of the project will increase, with further analyses built when the data is stored on a cloud service. 


SOURCE: United States Environmental Protection Agency (EPA). “Clean Air Markets Program Data.” Washington, DC: Office of Atmospheric Protection, Clean Air and Power Division. Available from EPA’s Air Markets Program Data web site: https://campd.epa.gov/.
