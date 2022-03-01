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

Google Data Studio based dashboard using two data sources:

1. **Standings** - Google Sheet with data exported from the database
2. **Games played** - directly from database

<screenshots>
