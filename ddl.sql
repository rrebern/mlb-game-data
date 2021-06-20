create table if not exists game(
    gamePK int PRIMARY KEY,
    gameDate DATE,
    venueId INT,
    awayTeamId INT,
    homeTeamId INT,
    awayTeamScore INT,
    homeTeamScore INT,
    topLaunchSpeed float,
    topLaunchSpeedPlayerId INT,
    totalPitches INT,
    recordLastModified timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE IF NOT EXISTS venue(
    venueId int PRIMARY KEY,
    venueName varchar(255),
    recordLastModified timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS player(
    playerId int PRIMARY KEY,
    playerName varchar(255),
    recordLastModified timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team(
    teamId int PRIMARY KEY,
    teamName varchar(255),
    recordLastModified timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
