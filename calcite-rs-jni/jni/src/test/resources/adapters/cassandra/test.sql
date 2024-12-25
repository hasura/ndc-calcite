-- Create the keyspace
CREATE KEYSPACE IF NOT EXISTS my_keyspace
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use the keyspace
USE my_keyspace;

-- Create the table
CREATE TABLE IF NOT EXISTS MusicPlaylist (
                                             SongId INT PRIMARY KEY,
                                             SongName TEXT,
                                             Year INT,
                                             Singer TEXT
);

-- Insert sample data
INSERT INTO MusicPlaylist (SongId, SongName, Year, Singer)
VALUES (1, 'Bohemian Rhapsody', 1975, 'Queen');

INSERT INTO MusicPlaylist (SongId, SongName, Year, Singer)
VALUES (2, 'Imagine', 1971, 'John Lennon');

INSERT INTO MusicPlaylist (SongId, SongName, Year, Singer)
VALUES (3, 'Hotel California', 1976, 'Eagles');