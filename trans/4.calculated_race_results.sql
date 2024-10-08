-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

create table f1_presentation.calculated_race_results
using parquet
as
select races.race_year,
       constructors.name as team_name,
        drivers.name as driver_name,
        results.position,
        results.points,
        11 - results.position as calculated_points
from results
join f1_processed.drivers on (results.driver_id = drivers.driver_id )
join f1_processed.constructors on (results.constructor_id = constructors.constructor_id)
join f1_processed.races on (results.race_id = races.race_id)
where results.position <= 10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

-- COMMAND ----------

spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              AS
              SELECT races.race_year,
                     constructors.name AS team_name,
                     drivers.driver_id,
                     drivers.name AS driver_name,
                     races.race_id,
                     results.position,
                     results.points,
                     11 - results.position AS calculated_points
                FROM f1_processed.results 
                JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
                JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
                JOIN f1_processed.races ON (results.race_id = races.race_id)
               WHERE results.position <= 10
                 AND results.file_date = '{v_file_date}'
""")

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

