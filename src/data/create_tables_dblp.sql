CREATE DATABASE dblp;

CREATE TYPE pub_type AS ENUM('article', 'inproceedings', 'proceedings', 'book', 'incollection', 'phdthesis', 'masterthesis', 'www');
CREATE TYPE venue_type AS ENUM ('journal', 'conference', 'book');
CREATE TYPE resource_type AS ENUM('ee', 'url', 'info', 'doi', 'stream', 'series');
CREATE TYPE ref_type AS ENUM ('crossref', 'cite');
CREATE TYPE aff_type AS ENUM('current', 'former');

CREATE TABLE IF NOT EXISTS Publications(
    key VARCHAR(255) PRIMARY KEY,
    mdate DATE NOT NULL,
    title TEXT NOT NULL,
    year int,
    month TEXT,
    type pub_type NOT NULL,
    school VARCHAR(255),
    isbn VARCHAR(255),
    pages VARCHAR(20),
    volume INT,
    number INT,
    venue_id INT,
    publisher_id INT,
    FOREIGN KEY(venue_id) REFERENCES Venues(id),
    FOREIGN KEY(publisher_id) REFERENCES Publishers(id)
);

CREATE TABLE IF NOT EXISTS Venues(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type venue_type NOT NULL
);

CREATE TABLE IF NOT EXISTS Resources(
    id SERIAL PRIMARY KEY,
    type resource_type NOT NULL,
    value VARCHAR(255) NOT NULL,
    publication_key VARCHAR(255),
    FOREIGN KEY (publication_key) REFERENCES Publications(key)
);

CREATE TABLE IF NOT EXISTS Publishers(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS Editors(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS PublicationEditors (
    publication_key VARCHAR(255),
    editor_id INT,
    PRIMARY KEY (publication_key, editor_id),
    FOREIGN KEY (publication_key) REFERENCES Publications(key),
    FOREIGN KEY (editor_id) REFERENCES Editors(id)
);

CREATE TABLE IF NOT EXISTS Reference(
    type ref_type,
    origin_pub VARCHAR(255),
    dest_pub VARCHAR(255),
    PRIMARY KEY (origin_pub, dest_pub),
    FOREIGN KEY(origin_pub) REFERENCES Publications(key),
    FOREIGN KEY(dest_pub) REFERENCES Publications(key)
);

CREATE TABLE PublicationAuthors(
    publication_key VARCHAR(255),
    author_id INT,
    PRIMARY KEY (publication_key, author_id),
    FOREIGN KEY (publication_key) REFERENCES Publications(key),
    FOREIGN KEY (author_id) REFERENCES Authors(key)
);

CREATE TABLE IF NOT EXISTS Authors(
    key SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NUll,
    id VARCHAR(4) NOT NULL,
    mdate DATE,
    UNIQUE(name, id)
);

CREATE TABLE IF NOT EXISTS AuthorWebsites(
    key SERIAL PRIMARY KEY,
    author_id INT,
    FOREIGN KEY(author_id) REFERENCES Authors(key),
    url VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Affiliations(
    id SERIAL PRIMARY KEY,
    author_id INT,
    FOREIGN KEY(author_id) REFERENCES Authors(key),
    affiliation VARCHAR(255),
    type aff_type
);

CREATE TABLE IF NOT EXISTS Alias(
    id SERIAL PRIMARY KEY,
    author_id INT,
    FOREIGN KEY (author_id) REFERENCES Authors(key),
    alias VARCHAR(255)
);