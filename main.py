import sys
import requests
from requests.exceptions import HTTPError
import re
import psycopg2
from psycopg2 import Error

def insertRow(gamePk, gameDate, venueId, venueName, awayTeamId, awayTeamName, awayTeamScore, homeTeamId, homeTeamName, homeTeamScore, launchSpeed, launchSpeedPlayerId, launchSpeedPlayerName, totalPitches):
    # UPSERT VENUE
    sqlVenue = "INSERT INTO venue (venueId, venueName) VALUES(" + str(venueId) + ", '" + venueName.replace("'", "''") + "') ON CONFLICT (venueId) DO UPDATE SET venueName = EXCLUDED.venueName;"

    # UPSERT TEAM
    sqlAwayTeam = "INSERT INTO team (teamId, teamName) VALUES(" + str(awayTeamId) + ", '" + awayTeamName.replace("'", "''") + "') ON CONFLICT (teamId) DO UPDATE SET teamName = EXCLUDED.teamName;"
    sqlHomeTeam = "INSERT INTO team (teamId, teamName) VALUES(" + str(homeTeamId) + ", '" + homeTeamName.replace("'", "''") + "') ON CONFLICT (teamId) DO UPDATE SET teamName = EXCLUDED.teamName;"

    # UPSERT PLAYER
    sqlPlayer = "INSERT INTO player (playerId, playerName) VALUES(" + str(launchSpeedPlayerId) + ", '" + launchSpeedPlayerName.replace("'", "''") + "') ON CONFLICT (playerId) DO UPDATE SET playerName = EXCLUDED.playerName;"

    # UPSERT GAME
    sqlGame = "INSERT INTO game (gamePk, gameDate, venueId, awayTeamId, awayTeamScore, homeTeamId, homeTeamScore, topLaunchSpeed, topLaunchSpeedPlayerId, totalPitches) VALUES(" + str(gamePk) +", '" + str(gameDate) + "', " + str(venueId) + ", " + str(awayTeamId) + ", " + str(awayTeamScore) + ", " + str(homeTeamId) + ", " + str(homeTeamScore) + ", " + str(launchSpeed) + ", " + str(launchSpeedPlayerId) + ", " + str(totalPitches) + ") ON CONFLICT (gamePk) DO NOTHING;"


    try:
        connection = psycopg2.connect(user="uqumrsnb",
                                      password="kxsdahpt",
                                      host="chcubs.crtnht6h1zib.us-east-1.rds.amazonaws.com",
                                      port="5432",
                                      database="uqumrsnb")
        cursor = connection.cursor()
        cursor.execute(sqlVenue)
        connection.commit()
        cursor.execute(sqlAwayTeam)
        connection.commit()
        cursor.execute(sqlHomeTeam)
        connection.commit()
        cursor.execute(sqlPlayer)
        connection.commit()
        cursor.execute(sqlGame)
        connection.commit()
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            # print("PostgreSQL connection is closed")

def loadGames(gameDate):
    if gameDate == "":
        gameDate = str(date.today())

    urlRoot = "http://statsapi.mlb.com"
    url = urlRoot + "/api/v1/schedule?sportIds=1&date="+gameDate
    totalGames = 0
    try:
        response = requests.get(url)
        response.raise_for_status()
        r = response.json()
        totalGames = r["dates"][0]["totalGames"]
        print("Total Games: ", totalGames)
        games = r["dates"][0]["games"]
        gameStatus = ''
        for i in games:
            gameStatus = i["status"]["statusCode"]
            abstractGameCode = i["status"]["abstractGameCode"]
            launchSpeed = 0.0
            launchSpeedPlayerId = 0
            launchSpeedPlayerName = ""
            if gameStatus == 'F' or gameStatus == 'F':
                resp = requests.get(urlRoot + i["link"])
                live = resp.json()
                pitchesStrikes = ""
                h = 0
                lenInfo = len(live["liveData"]["boxscore"]["info"])
                while h < lenInfo:
                    if live["liveData"]["boxscore"]["info"][h]["label"] == "Pitches-strikes":
                        pitchesStrikes = live["liveData"]["boxscore"]["info"][h]["value"]
                    h += 1
                allPlays = live["liveData"]["plays"]["allPlays"]
                lenAllPlays = len(allPlays)
                j = 0
                while j < lenAllPlays:
                    eventType = allPlays[j]["result"]["eventType"]
                    if eventType == "single" or eventType == "double" or eventType == "triple" \
                        or eventType == "home_run" or eventType == "field_out" or eventType == "sac_bunt" \
                        or eventType == "field_error" or eventType == "double_play" \
                        or eventType == "grounded_into_double_play" or eventType == "fielders_choice" \
                        or eventType == "fielders_choice_out" or eventType == "force_out" \
                        or eventType == "grounded_into_double_play" or eventType == "grounded_into_triple_play" \
                        or eventType == "triple_play" or eventType == "sac_fly" or eventType == "sac_fly_double_play" or eventType == "sac_bunt_double_play":
                            lenEvents = len(allPlays[j]["playEvents"])
                            if "launchSpeed" in allPlays[j]["playEvents"][lenEvents - 1]["hitData"]:
                                if launchSpeed < allPlays[j]["playEvents"][lenEvents - 1]["hitData"]["launchSpeed"]:
                                    launchSpeed = allPlays[j]["playEvents"][lenEvents - 1]["hitData"]["launchSpeed"]
                                    launchSpeedPlayerId = allPlays[j]["matchup"]["batter"]["id"]
                                    launchSpeedPlayerName = allPlays[j]["matchup"]["batter"]["fullName"]
                    j += 1

                # EXTRACT TOTAL PITCHES FROM PITCHES-STRIKES STRING
                newPitches = re.split('-', pitchesStrikes)
                newPitches.pop()
                lenPitchList = len(newPitches)
                l = 0
                totalPitches = 0
                while l < lenPitchList:
                    m = re.search(r'\d+$', newPitches[l])
                    if m is not None:
                        # print(m.group(0))
                        totalPitches += int(m.group(0))
                    l += 1
                # SEND FLAT DATASET TO POSTGRES WHERE IT WILL BE SPLIT INTO GAME(FACT) AND VENUE, TEAM AND PLAYER DIMENSIONS
                insertRow(i["gamePk"], gameDate, i["venue"]["id"], i["venue"]["name"],
                          i["teams"]["away"]["team"]["id"], i["teams"]["away"]["team"]["name"],
                          i["teams"]["away"]["score"],  i["teams"]["home"]["team"]["id"],
                          i["teams"]["home"]["team"]["name"], i["teams"]["home"]["score"],
                          launchSpeed, launchSpeedPlayerId, launchSpeedPlayerName, totalPitches)

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')


if __name__ == '__main__':
    # print_hi('PyCharm')
    try:

        arg = sys.argv[1]
        loadGames(str(arg))

    except IndexError:

        raise SystemExit(f"Usage: {sys.argv[0]} YYYY-MM-DD")
