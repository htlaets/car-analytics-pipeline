DO $$
DECLARE
    empty_tables text[];
BEGIN
    SELECT array_agg(table_name)
    INTO empty_tables
    FROM (
        SELECT 'mart_avg_metrics_by_year' AS table_name
        WHERE NOT EXISTS (SELECT 1 FROM car_analytics.mart_avg_metrics_by_year)
        UNION ALL
        SELECT 'mart_avg_metrics_by_origin'
        WHERE NOT EXISTS (SELECT 1 FROM car_analytics.mart_avg_metrics_by_origin)
        UNION ALL
        SELECT 'mart_avg_metrics_by_cylinders'
        WHERE NOT EXISTS (SELECT 1 FROM car_analytics.mart_avg_metrics_by_cylinders)
        UNION ALL
        SELECT 'mart_top_power_to_weight'
        WHERE NOT EXISTS (SELECT 1 FROM car_analytics.mart_top_power_to_weight)
    ) checks;

    IF empty_tables IS NOT NULL THEN
        RAISE EXCEPTION 'Empty marts: %', empty_tables;
    END IF;
END $$;
