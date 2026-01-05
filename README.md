# data-engineer-interview-danske-bank
Take home task related to job interview at Danske Bank

## Use of AI
Throughout the project GitHub copilot has been used for code completion as well as for PR reviews. Apart from this, no
use of AI has been involved in creating the solution.

## Considerations
For this task the NYC Taxi data set has been chosen.

The solution consists of
- A class for data IO
- A pipeline with the following steps
  - Reading of fact and dimensional data
  - Cleansing of fact data
  - Joining of dimensions on the fact data
  - Aggregation showing grouping and windowing of data to provide the optimal vendor to choose for trips between combinations of zones in NYC on three different parameters: Lowest average price per distance, lowest average trip duration and finally highest number of trips.

  The final report is saved as parquet file in the output directory.

For data processing the Polars library was chosen. The reason for this is, that Polars is high performant on medium sized datasets while being lightweight enough to run on a standalone computer. Moreover the syntax for Polars to a great extent resembles that of Apache Spark.

The pipeline has been created as a python class incorporating a launch function. This structure is chosen as it aligns with how Databricks Asset Bundles most easily handles task deployments.