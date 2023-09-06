# Presto SQL Parser

## Introduction

### What is the data lineage?
Data lineage refers to the ability to track and trace the origin, location, movement, and transformation of data as it flows through various systems, processes, and applications. It provides a comprehensive view of data's lifecycle, including its sources, transformations, and target systems. Data lineage helps organizations understand where data comes from, how it has been modified, and where it is being used. It is crucial for data governance, compliance, data quality management, and data analytics.


### Why is data lineage important in a big data platform?
Data lineage is particularly important in big data platforms due to the sheer volume, velocity, and variety of data involved. In big data environments, data is often sourced from multiple systems, undergoes complex transformations, and is distributed across various analytics, reporting, and storage systems.

Data lineage in big data platforms provides the following benefits:

1. Data understanding and trust: It helps users comprehend the origin and quality of data. By tracing the data's path, users can validate its reliability and make informed decisions based on accurate information.

2. Data governance and compliance: Data lineage aids in meeting regulatory requirements by providing visibility into data sources and transformations. It enables organizations to demonstrate data lineage for auditors and regulatory bodies, ensuring data integrity and compliance.

3. Data quality management: By tracking data from its source to its destination, data lineage assists in identifying and addressing data quality issues. It enables organizations to highlight and rectify data inconsistencies, anomalies, or errors at various stages of data processing.

4. Impact analysis and change management: Understanding data lineage helps assess the impacts of changes to data sources, structures, or transformations. It allows organizations to evaluate the potential consequences of modifications and plan accordingly, minimizing risks and ensuring smooth change management efforts.

5. Data analytics and troubleshooting: Data lineage aids in troubleshooting data-related issues by identifying the source of anomalies, discrepancies, or errors. It accelerates the root cause analysis process by providing a clear view of the data's transformation journey.

In summary, data lineage is crucial in big data platforms to ensure data understanding, governance, compliance, quality management, impact analysis, and troubleshooting, enabling organizations to effectively manage and leverage their data assets.


### Introduction to this project

This project is used to analyze the data lineage of PrestoSQL. With this project, you can obtain information about the source tables in PrestoSQL and which columns are selected. This allows for table/column-level data lineage analysis. By writing the parsed data lineage into Hive tables, it becomes possible to further analyze PrestoSQL running on the current big data platform using a big data computing framework. This enables better data modeling and governance.


## How to Use?
Please package the code as a JAR file and use it as a User-Defined Function (UDF).
The input for this UDF should be PrestoSQL, and the output should be the data lineage of that PrestoSQL query.

`add jar hdfs://R2/user/yadong.liu/sql_v1.1.jar ;`

`create or replace temporary function getTableRelation as 'com.winter.sqlparser.lineage.GetTableRelationUDF';`
