# Chess.com Division Tracker

This project was built to support IM Damian Lewtak while playing in chess.com [Champion league](https://www.chess.com/news/view/chesscom-releases-leagues).

## Components

Project consists of three components, based around a PostgreSQL database.

### Data load

Python modules supporting data loads from chess.com public and not-so-public APIs, in the [code directory (with their own README)](code/).

Following chess.com data is fetched:

- league standings (ranking & score)
- list of games played for users of interest

Additionally, a helper script to export processed standing data from the database into a Google Sheet was implemented to facilitate the fact Google Data Studio supports a 15 minute refresh interval on top of GSheet data, compared to 1 hour off a Postgres database.

### Database model

[dbt™](https://www.getdbt.com/) database model transforming the raw data and calculating measures to support Google Data Studio dashboard.

Following functionalities are implemented on top of standing data:

- rounds timestamps to the nearest minute
- calculates score delta over 1 hour, 1 day and 2 days
- calculates player downtime

Following functionalities are implemented on top of player games data:

- determine division player's and their oppontent's color, ranking, result
- calculate longest win streaks for players participating in the division

### Dashboard

[Dashboard - data as of Feb 12th, 20:50 CET, frozen in time.](https://datastudio.google.com/reporting/e57b9275-2de3-4f8e-b257-e71bdd37a219)

Google Data Studio based dashboard using two data sources:

1. **Standings** - Google Sheet with data exported from the database
2. **Games played** - directly from database

![image](https://user-images.githubusercontent.com/2545419/156154515-ac645c1f-c351-4b9a-bd64-355ecf128010.png)
![image](https://user-images.githubusercontent.com/2545419/156153338-7ef09248-d4b0-4e69-ac44-5dfb03a120b0.png)
![image](https://user-images.githubusercontent.com/2545419/156153480-bd4cde90-9b98-4e76-9b0d-8dfe29277d22.png)
![image](https://user-images.githubusercontent.com/2545419/156153572-f08c294d-f6d8-4b70-af8f-d46a0ceba4c2.png)
![image](https://user-images.githubusercontent.com/2545419/156153634-2e3a9cdd-6b0d-4205-a032-63fe4e05e6a5.png)
![image](https://user-images.githubusercontent.com/2545419/156153669-2f8c6dd6-97d5-4aa8-9927-87314bf65d9c.png)

