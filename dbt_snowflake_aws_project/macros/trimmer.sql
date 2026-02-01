{% macro trimmer(column_name, node) %}
    {{ column_name | trim | upper}}
{% endmacro %}

--> here we are not using the node variable but we can use it to get the context of the model where this macro is being called from... like we can get the model name, schema name etc...
--> In dbt, a node is a dictionary containing all the metadata (the "identity card") for a model, source, or seed. It includes details like: 
{# node.name: The name of the model currently being run.
node.path: The file path where the model is stored (e.g., models/staging/schema.yml).
node.config: Settings like whether the model is a table or a view.
node.tags: Any labels you've attached to the model in your .yml files.  #}


{# {% macro trimmer(column_name, node) %}
    {# Only UPPERCASE if the model name contains 'staging' #}
    {% if 'staging' in node.name %}
        {{ column_name | trim | upper }}
    {% else %}
        {{ column_name | trim }}
    {% endif %}
{% endmacro %} #}