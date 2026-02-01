--> first create a uv project
uv init

--> create a virtual environment.... to change the python version we can go at .python-version
uv sync

--> to activate the virtual environment at windows:- virtual environment name would be same as folder name.
.venv/Scripts/activate
for mac:- source .venv/bin/activate

--> add dbt-core package using uv (similar to pip install)
uv add dbt-core 

--> to delete a venv:-
    deactivate
    rm -rf .venv
    ls -a

--> we will need an adapters to work with some warehouses
    uv add dbt-snowflake 
    uv add dbt-databricks

--> now let's create our first commit... to make sure all the changes are being tracked
    git branch -m main ==> if we are having the older git installed in our system... by default git was having master branch... we are renaming it as main branch

    git add .
    git commit -m "inital commit"
    
--> create a feature branch for our project
    git switch -c feature-aadil

--> to list the git branches
    git branch
    
--> check if our branch is already activated... or we can activate it if we want...

--> start the dbt project in the current virtual env.
 dbt init

--> give a project name
--> select adapaters using 1,2,3,...

--> now it will ask snowflake account details 
    --> account identifier
    --> username
    --> authenticate --> using password --> add snowflake password
    --> role
    --> warehouse name by default it's COMPUTE_WH
    --> database name
    --> schema :- dbt_schema (it will be a new schema)

--> A profile would be created and written to C drive --> that is nothing but a authentication file for dbt local to snowflake..
            --> token --> host --> database-name
--> we don't push this file to github.
--> remember, we need to bring this file from our c drive to newly created project folder...
--> so that we can see what is happening in that file via (copy file and paste)
--> we would push this profile file too to the github... but hiding the creds....
--> yeh file batayengaa ki kya kya hua tha while creating the project.

Configuration Management: The profiles.yml file defines how DBT connects to your database, including credentials, database name, schema, etc. Keeping it within the project's structure ensures that all necessary configurations are readily available for the project to run 

--> what does the DBT does:-
    => move the data from 1 place to another layer (even when while doing the transformations)
    => suppose layer 1 we have views --> layer 2 we have tables.
    => just define select statements and transformations and materiliaztions type..
    => dbt says, I will do everything then..

--> now, remember to instal the power users for dbt extensions... to remove all red-red errors in our files.
--> this will make our normal sql and python and yaml files as jinja files...
--> this part can be seen in the project tutorial at youtube... 
        press cmd + , --> settings --> files associations --> check if sql and yaml are being considered as jinja yaml or not.

===================================================================================================================================
### MAIN DBT FILES:-
    1. dbt_projects.yml
    2. 

### configurations hierarhcy:-
    Inline config (code level) -> model level (properties.yml) --> dbt_project.yml files.
===================================================================================================================================
--> we created a 1 demo folder and write a select * on the staging folder.
--> then we can run the sql files directly by the dbt run as we run the normal code...
--> or we can try to run the models sepeeratly too.

--> make sure we are in the project folder before running any dbt commands.
-> to run all the models:- dbt run
-> to run the specific model:- dbt run --select models/demo

--> we will find a model as example... we can delete that...
--> remember we are having a schema defined by default in the "profiles.yml" files..
--> any dql query we write in the model... would create a view( by defualt materialization) in the "profiles.yml" file.
--> it would use the by default schema and database which is written in the "profiles.yml" files
--> to change it... we can go ahead and write our own materializations in the "dbt_project.yml" files.

--> whenever we are running the sql models... using dbt cli --> remember we cannot have any semi-colon.. else we will get the error...
--> once the query is successfull --> we will be able to see that new schema is already created --> new views are already created....

--> then we can create the configs at the model level.

### Note:-
        1. when we just do select * in our model a view/table is created.
        2. when we rerun the model... all previous tables created are deleted sometime it does not delete(not-sure why??)
        3. the sql files name which we write under the sql files, with the same name our tables/views are created.

===================================================================================================================================
                                                    Source
For source we will have to create a folder under models.
source is best for tracking the lineage graph.
source name and schema is name is same only..
sources needs yaml files only..
don't use tabs in the yaml files.
hierarhcy is very much important in case of yaml files.
treat yaml file as key value pair json files with string as lists..



## Remember that our source was available in the staging layer..
## now, we can refer the source that was created under sql files using jinja source macros.
## Lineage get's created as soon as we complete the sources.. no need to run the dbt run command... it's a real time.
## source jaise hi sql mai daala right top corner mai option aa rha honga... to compile and check the sql files... please do click and check if everything is fine.


                                                    properties.yml
1. put under the models folder 
2. bronze,silver,gold ke liye alag-alag 3 properties files aayegii so that we can even define ki bro is model mai is table ko keep it as view, isko table etc.
2. define the model and it's materiliaztions

                                                    dbt compile
identify the errors before running the model and then we can use the dbt run 


===================================================================================================================================
                                        now, how to define different schema for different models
--> go to dbt_project.yml file and add the +schema arguments to add the different schema for different models.

                                        how to remove the by default "schema name" as <dbt_schema>
--> this comes, since dbt use a inbuild schema called as generate_schema_name

--> now, we will just have to go and create a new macro to override the inbuilt macro...
    keep the macro file name as generate_schema_name.sql file under macro
    also, remove the prefix of the macro from last third line.


 ===================================================================================================================================
                                                    jinja
--> used to add dynamic facility to sql models.
--> always check the compile screen, whenever we add the jinja
--> ref:- to refer any model into suppose test.sql  ................ adhoc analysis we have folder called as "analysis"

# UseCases:-
    1. setting up the variables....
            
        {% set nights_booked = 1 %}

        SELECT * FROM {{ ref ('bronze_bookings') }} WHERE NIGHTS_BOOKED > {{ nights_booked}}

    2. Else Elif in the sql...
        run a conditional where clause (if/else)

    3. For Loops...
        just selecting the columns from a table which are available in a specific list.
            --> last column might have a extra comma .. which can be seen in the compile file...
            --> let's remove that comma..

    And Many More....

 ===================================================================================================================================
                                                    incremental Loading the data using jinja

--> this jinja is used to run the default model which is available at the dbt_project.yml files....

--> {% set incremental_flag = 1 %}
{% set incremental_col = 'CREATED_AT' %}

SELECT *
FROM {{ source('staging', 'bookings') }}

{% if incremental_flag == 1 %}
WHERE {{ incremental_col }} >
(
    SELECT COALESCE(MAX({{ incremental_col }}), '1900-01-01')
    FROM {{ ref('bronze_booking') }}
)
{% endif %}


-- when we will run the model manuallly... we need to pass the ref('bronze_booking') model... but if we want to run the model using dbt run/dbt compile
-- we would need {{this}} ... to ref the current model.

khud ko khud hi mai refer karna for that we need this macro..

-- inplace of passing the incremental flag we can also use the incremental materiliaztions in model config and use is_incremental() at the query level...

--> THE above THING should be ran for the sqls files which are available in the models folder only... and not on the analysis folder sql files. becuase analysis folder sql files are not part of the dbt run/compile process... they are only ran when we run them manually... 
--> and this is the reason why we cannot use {{this}} in the analysis folder sql files... we need to use ref('model_name') in the analysis folder sql files...

====================================================================================================================================================================
                                            TARGET FOLDER
we will see the compiled and run folders.
open it and we will see that what query does dbt is sending to the snowflake/databricks..

this confirm that in dbt we just write DQL and materiliaztions by that DML and DDL are taken care by dbt itself.

====================================================================================================================================================================
                                            macro 

macro are just a functions, which we can create by our own, so if we want to do some repetative calculations, we can create a macro.
we can call the macro in the models using jinja
point to be noted that, the columns paramter need to be passed under 'inverted-quotes'

if we want to update a existing internal macro.. we can do that do... for that create a file with the same macro name and update the macro...

please check the macro folder now...
====================================================================================================================================================================
                                            upsert in silver layer (incremental code + key column)

when we define the materilization in models.. and we pass a key column with incremental materiliaztions... it becomes a upsert.

upsert means :- do plain insert for new keys... do updates for older keys.

--> we created 3 more macros:- tag, multiple and trimmer ...
                                    In trimmer we are using node... for future use case.
                                                    node is nothing but a dict of key, value pairs having the details of the current model... currently we are not using that.
                                                    we are using filters (jinja have some inbuild functions, we can apply that to the column like trim, upper etc.)

=====================================================================================================================================================================
                                            Filters in Jinja
--> Jinja have filters... SQL execution se pehle apply hote hain
🔹 Common DBT / Jinja Filters
1️⃣ | lower, | upper
{{ 'AADIL' | lower }}
-- aadil

2️⃣ | replace
{{ 'aadil_mansoori' | replace('_', ' ') }}
-- aadil mansoori

3️⃣ | trim
{{ '  aadil  ' | trim }}


⚠️ Yeh Jinja-level trim hai, SQL ka nahi

4️⃣ | default
{{ var('env_name') | default('dev') }}


Agar variable nahi mila → dev

5️⃣ | join
{{ ['id', 'name', 'age'] | join(', ') }}


Output:

id, name, age

===========================================================================================================================================
                                                        NODES in DBT

1️⃣ Pehle yeh samajh: Node hota kya hai?

DBT internally har cheez ko ek “node” maanta hai:

    model

    test

    snapshot

    seed

Aur har node ke paas metadata hota hai jaise:

    model ka naam

    schema

    database

    materialization

    tags

    config

    file path

👉 Isi metadata object ko node bolte hain


Kabhi-kabhi hum khud pass karte hain:

{% macro my_macro(node) %}
    {{ log(node.schema, info=True) }}
{% endmacro %}

5️⃣ this vs node (bahut common confusion)
Concept	Kya hai
this	Current model ka SQL relation
node	Current model ka metadata

==============================================================================================================================================
--> we are creating a new macro for the silver layer... to apply a chain of transformations...
{% macro trimmer(column_name, node) %}
 {{ col_name | trim | upper}}
{% endmacro %}

--> macro for if and else....
{% macro tag(col) %}
    { % if col ‹ 100%}
        low
    {% elif col ‹ 200 %}
     medium
    {% else %} 
        high 
    {% endif% }

--> in place of writing like this we should go ahead and write in sql format.

{% macro tag(col) %}
    case
        when {{ col }} < 100 then 'low'
        when {{ col }} < 200 then 'medium'
        else 'high'
    end
{% endmacro %}

============================================================================================================================
                                                Silver Layer (Remember any folder we are adding, we need to add the materializations, in the dbt-project.yml files.)
--> for bookings we are using some macros.... for hosts.... we are having sqls transformations.

============================================================================================================================
                                                Gold Layer
--> for gold we are creating One big table (applying joins) || using a jinja templates. || using dynamic functions..
---> Then this OBT is break down into facts and dimensions tables.


===> we created a file as obt.sql --> added the metadriven pipeline concepts...

→ suppose we are having 1 big table created using joining 3 tables.
→ now, what happen 1 more table came.. Now we will have to make the changes in the join query.
→ but due to the meta-data driven pipelines… we just make the changes in configs and add the metadata….
→ this makes the code reusable and parameterised.
→ this is why dbt is famous for.

⇒ Configuration is a list of array… where we will put the details of the configs in the list.

--> to see any tables data just do select * from { ref the model } ... in the analysis folder ....while development.

================================================================================

Now, in the configs list which we created in the obt.sql files…
We can access the configs using list indexing….
Or we can also do 1 thing ….. | to use loop.start/loop.end to get the first and last elements of the lists.

SELECT
    {% for config in congigs %}
        {{ config. columns }}% if not Loop.last %}, {% endif %}
    {% endfor %}
FROM
    {{ congigs[0].table }} AS {{ congigs[O].alias }}
        {% for config in congigs[1:] %}
            LEFT JOIN {{ config.table }} AS {{ config.alias }}
                ON {f config-join_condition }}
    {% endfor %}

Using Jinja write the Join Query …
Here loop.last is to make sure, we don’t add a comma at last.
Loop.first is representing the “From” Clause.
Else… is representing the all other tables which would be joining.
We are just saying ki if there is only 1 table go ahead and just select the tables.. If another tables are present , then do the left join…

=====================================================================================
Then we moved ahead for creating the facts and dimensions tables in gold layer.
=====================================================================================
                                                    Snapshots

it is a dbt internal meta driven concepts...

Now, adays writing code for scd is not so good….
	To make sure… we stop using the repeatative code….
	This framework have came into the picture.

Snapshots are nothing but configurations in dbt.

for this we have a sepearate folder to achieve this....

Remember that → facts will have numerical values..	
			→ dimensions would have contextual values, primary key, and date.

Now, for creating the snapshots, we would be needing the source right??

Now, the questions comes into the mind, ki  bro if we are creating the source for the dimensions and facts tables… aren’t we duplicating the tables again and again... since the source would be same only???

Yes, for this comes the ephemeral models concepts in dbt.

This models are not created into the databases, the queries are just written into the code… and when the child model have the dependencies of this parent models…. They just run the queries whenever required… but no data storage into the dbt.

====================================================================
                                            Ephemaral models (This are similar to the CTEs, we will write once and import it multiple times.)
now, inplace of creating multiple duplicate source for the dimensions tables... we would be creating the ephemeral source for dimensions tables.
    --> and since in our dimesnion tables... we just need incremental data
    --> we would be creating snapshots .yaml files for our dimensions..
    --> this .yaml files would be reading the data from the ephermeral source.
Ephemeral ke liye bhi we would need to add the materializations in  projects.yml models.

### SNAPSHOTS
Relation → means source
Unique_key → on which column || scd type 2 would be applied.
Stretagey: timestamp. (since in our current data we have the time-stamp for incremental load... else we would have gone ahead with the key columns....)
Updated_at : created_at
Dbt_valid_to_current : “to_date(‘9999-12-21’)
						
we need to use a special comands to run the snapshots files...	Dbt snapshot is used to run the snapshot.. We can also use the dbt build (dbt run + dbt test)
=====================================================================================================================================
Dbt clean:- to clean everything in the target folders….

Dbt snapshots folder contains the .yml files… generally for building the scd type 2 dimensions tables…

Then create the facts tables… (Dimensions tables are available in the snapshot folders... while fact table is available in the gold layer....)

So dim_bookings.yml, dim_hosts.yml etc…. Are the snapshots files with scd-type 2 notes.

Booking_ephemral_source.sql, hosts_ephemral_source.sql in the ephemeral folder are the source…

									Fact Table
Obt would be the source for the fact table
Only numerical column and pks would be landing here.
Dates generally goes to dimensions.

Create the fact tables using obt code as of now.

								DBT Tests ( we try to generally add the tests at the source)

Singular and generic tests
Applying on source, so bad data flows.

We can define the severity:-
Error
Warning 

--> if we define test and it passes.. we get error.
--> if we don't want any error but just a warning.... please go ahead and add the config as warning

								Push the code
Go to main project folder → git add . → git commit -m “project done” → git switch main → git branch → git merge feature_aadil


==> extra features:-
    1. adding seeds
    2. adding generic source
    3. adding freshness.

