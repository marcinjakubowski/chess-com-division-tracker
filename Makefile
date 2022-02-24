.DEFAULT_GOAL := dev

dev:
	dbt seed
	dbt run
	dbt test
	
prod:
	dbt seed -t prod
	dbt run -t prod
	dbt test -t prod