# Data Lakehouse Architecture

## Introduction

Welcome back.

Before we start working on Databricks, let's try to understand the solution that Databricks helps us deliver.

Databricks basically enables us to build a modern data Lakehouse platform.

The concept of data Lakehouse could be new to most of you, so let's first explore the concept of the Data Lakehouse architecture.

While the term data Lakehouse was originally coined by Databricks, it's now widely adopted by many organizations.

According to Databricks, a data Lakehouse is a new open data architecture that merges the flexibility, cost, efficiency, and scalability of data lakes with the data management and asset transaction capabilities of the data warehouses.

This combination supports both business intelligence and machine learning workloads on a unified platform.

As you can see, the Data Lakehouse is not entirely a new concept.

It essentially blends the architecture of a data lake with some of the core characteristics of data warehouses.

This combination helps create a robust data platform which is suited for handling both BI and ML workloads efficiently.

---

## Introduction to Data Warehouses

I understand that some of you might not be familiar with data warehouses and data lakes, so let's begin with an introduction to data warehouses.

Data warehouses emerged during the early 1980s, when businesses needed a centralized place to store all their organizational data.

This allowed them to make decisions based on the full set of information available in the company, rather than relying on separate data sources in individual departments.

A data warehouse primarily consisted of operational data from within the organization.

Some large data warehouses also gathered external data to support better decision making.

The data received in a data warehouse was usually structured, such as SQL tables or semi-structured formats such as JSON or XML files.

However, they couldn't process unstructured data such as images and videos.

The data received then goes through a process called ETL or extract transform load to be loaded into a data warehouse.

Complex data warehouses also had data marts which focused on specific subject areas or regions of the business.

Data marts contained cleaned, validated, and enhanced data, often aggregated to provide key insights like key performance indicators or KPIs.

Analysts and business managers could then access this data through business intelligence reports.

By the early 2000, most large companies had at least one data warehouse because they were crucial for making business decisions.

---

## Challenges with Traditional Data Warehouses

However, they also had several significant challenges.

With the growth of the internet, data volumes increased dramatically and new type of unstructured data like videos, images, and text files became important for decision making.

Traditional data warehouses were not designed to handle these type of data in a data warehouse.

Data was only loaded after its quality was checked and transformed.

This led to longer development times to add new data into the data warehouse.

Data warehouses were built on traditional relational databases or massively parallel processing engines or MPP engines, which used proprietary file formats and could create vendor lock in.

Traditional on premises data warehouses were hard to scale, sometimes requiring large migration projects to increase capacity.

Storage was expensive with these traditional solutions, and it was impossible to expand storage independently of computing resources.

And finally, traditional data warehouses didn't provide enough support for data science, machine learning, and AI workloads.

Those issues paved the way for data lakes.

---

## Introduction to Data Lakes

Data lakes were introduced around the year 2011 to address some of the challenges we saw with the data warehouses.

Data lakes are designed to handle not only structured and semi-structured data, but they can also handle unstructured data.

Unstructured data makes up roughly 90% of the data available today in a data lake house architecture.

Raw data is ingested directly into the data lake without any initial cleansing or transformation.

This approach allowed for quicker solution development and faster ingestion times.

Data lakes were built on cheap storage solutions like HDFS and cloud object stores such as Amazon S3 and Azure Data Lake Storage Gen2, which kept the costs low.

They also utilized open source file formats like parquet, ORC and Avro, allowing for a wide range of tools and libraries to be used for processing and analysis.

Data lakes supported data science and machine learning workloads by providing access to both raw and transformed data.

However, there was one major problem.

Data lakes were too slow for interactive BI reports and lacked proper data governance.

To solve this, companies often copied a subset of the data from the data lake to a warehouse to support BI reporting, which led to a complex architecture with too many moving parts.

---

## Challenges with Data Lakes

Now that we understand the basics of the Data Lake architecture, let's summarize its challenges.

Data lakes did not have built in support for asset transactions, which are essential for reliable data management.

In case you're not familiar with the acronym Acid, it stands for atomicity, consistency, isolation, and durability, which is essential for reliable data management.

Lack of acid transaction support led to many issues.

And let's go through some of those here.

Fail jobs could leave behind partially loaded files requiring additional cleanup processes during reruns.

There was no guarantee of consistent reads, leading to the possibility of users accessing partially written data, which compromised reliability.

Data lakes offered no direct support for updates to correct or update data.

Developers had to partition the files and rewrite entire partitions, which was both time consuming as well as error prone.

There was no way to roll back changes, which made it difficult to recover from failures.

Under GDPR, users have the right to be forgotten, which means that their data must be deleted when requested.

Since the data lakes didn't support deletions well, entire files sometimes had to be rewritten to remove an individual's data, which was again both time consuming and expensive.

Data lakes lacked version control, which made it harder to track changes, perform rollbacks, or ensure data governance.

Also, data lakes struggled to provide fast, interactive query performance and lacked adequate support for basic needs such as security and governance.

Setting up and managing data lakes was complex and required significant expertise in a data lake.

Streaming and batch data needed to be processed separately, leading to complex lambda architectures.

---

## Data Warehouses vs Data Lakes

In summary, data warehouses excelled at handling BA workloads, but they lack the support for streaming data science and machine learning workloads.

On the other hand, data lakes were designed for data science and machine learning workloads, but they fell short in supporting BA workloads effectively.

---

## Data Lakehouse Architecture

The Data Lakehouse architecture combines the best features of both data warehouses and data lakes.

It is designed to effectively support business intelligence and data science, machine learning and AI workloads.

Let's take a closer look at how a data Lakehouse architecture works.

Similar to data lakes, we can ingest both operational and external data into a data lake house.

A data lake house is essentially a data lake with built in acid transaction controls and data governance capabilities.

Data lakes achieve this using the file format Delta Lake and a data governance solution called Unity Catalog.

With Acid support offered by the Delta Lake file format, we can seamlessly combine streaming and batch batch workloads, eliminating the need for a complex Lambda architecture.

Data from the Lakehouse platform can be used for data science and machine learning tasks.

Additionally, the Lakehouse integrates with the popular BI tools such as power BI and Tableau, while also providing role based access control for governance.

This eliminates the need to copy the data into a separate data warehouse.

---

## Benefits of Data Lakehouse Architecture

Let's now quickly summarize the benefits of using the Data Lakehouse architecture.

Similar to traditional data lakes, lake houses can also handle all types of data such as structured, semi-structured, and unstructured.

Data Lake houses run on cost effective cloud object storage, such as Amazon S3 or Azure Data Lake Storage Gen2 using the open source file format such as Delta Lake.

They support a wide range of workloads, including by data science and machine learning.

Data lake houses allow direct integration with BI tools, which helps eliminate the need for duplicating the data into a data warehouse.

Most importantly, they provide asset support, data versioning, and history, thus preventing the creation of unreliable data swamps.

Lake houses offer better performance compared to traditional data lakes.

And finally, by removing the need for a lambda architecture and reducing the reliance on separate data warehouses, lake houses simplify the overall architecture.

---

## Conclusion

I hope you now have a good understanding about Data Lake House architecture.

And that's the end of this lesson.

I'll see you in the next one.

---
