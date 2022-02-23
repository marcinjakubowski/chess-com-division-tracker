import argparse

from db import Db
import api


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import chess.com league and games data for a given player')
    parser.add_argument('username', help='chess.com username')

    args = parser.parse_args()
    username = args.username

    db = Db()
    division = api.get_player_division(args.username)
    division = db.session.merge(division)
    db.session.commit()

    if not division.is_active:
        exit(1)


    # standings take priority over games, so fetch those first
    # it's not really important if games fail as they will all get fetched 
    # the next time anyway
    standings = api.get_division_data(division.level, division.id)
    db.add_standings(standings)

    # ensure games are downloaded for the player passed from the argument
    # as well as any other players specified in the division
    # use case folding as the username case in the db and from the arg can differ
    all_players = [username] if (p := division.players) is None else p + [username]
    players = set({v.casefold(): v for v in all_players}.values())

    for player in players:
        games = list(map(lambda g: {**g.to_dict(), "division": division.id},
                            api.get_user_games(player, division.start_time, division.end_time)))
        db.add_games(games)





