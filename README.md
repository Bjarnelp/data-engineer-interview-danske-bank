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
