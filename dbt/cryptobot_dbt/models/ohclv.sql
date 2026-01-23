WITH source_data AS (

    SELECT
        (date / 1000)::TIMESTAMP AS date,
        symbol,
        open,
        high,
        close,
        low,
        volume
    FROM {{ source('raw_data', 'raw_ohclv') }}
)

SELECT * FROM source_data