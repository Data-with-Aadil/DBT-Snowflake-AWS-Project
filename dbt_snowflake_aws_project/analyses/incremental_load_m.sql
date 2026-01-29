-- This model implements a manual incremental load using Jinja variables. 
-- It reads data from the staging.bookings source table and conditionally applies a filter based on an incremental_flag.
--  When the flag is enabled, the model only selects records whose CREATED_AT timestamp is greater than the maximum CREATED_AT value already present in the bronze_bookings table,
-- ensuring that only new records are loaded on subsequent runs. 
-- The COALESCE with a default date (1900-01-01) ensures that on the first run, when the target table is empty, all historical data is loaded. 
-- This logic helps avoid reprocessing old data but does not handle updates, deletes, or late-arriving records and is not dbt’s native incremental mechanism, as it relies on manual Jinja conditions rather than is_incremental() and materialized='incremental'.

{% set incremental_flag = 1 %}
{% set incremental_col = 'CREATED_AT' %}

SELECT *
FROM {{ source('STAGING', 'bookings') }}

{% if incremental_flag == 1 %}
WHERE {{ incremental_col }} >
(
    SELECT COALESCE(MAX({{ incremental_col }}), '1900-01-01')
    FROM {{ ref('bookings') }}
)
{% endif %}


-- since the data was already loaded to our target table.... so no record inserted... we can truncate the table and then reload the data.

-- when we will run this model directly by the run button, we need to use the ref('bronze_booking'), but if we are doing dbt compile or dbt run from the terminal, we need to use {{ this }} instead of ref('bronze_booking') because during the compilation phase, 
-- dbt replaces {{ this }} with the actual table name in the database, ensuring that the incremental logic works correctly in both direct runs and full project executions.

---> THE BELOW THING should be ran for the sqls files which are available in the models folder only... and not on the analysis folder sql files. becuase analysis folder sql files are not part of the dbt run/compile process... they are only ran when we run them manually... 
----> and this is the reason why we cannot use {{this}} in the analysis folder sql files... we need to use ref('model_name') in the analysis folder sql files...

-- -- when we will run the model manuallly... we need to pass the ref('bronze_booking') model... but if we want to run the model using dbt run/dbt compile
-- we would need {{this}} ... to ref the current model.

-- khud ko khud hi mai refer karna for that we need this macro..

-- inplace of passing the incremental flag we can also use the incremental materiliaztions in model config and use is_incremental() at the query level...