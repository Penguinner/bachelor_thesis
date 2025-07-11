CREATE TYPE pub_type AS ENUM('article', 'inproceedings', 'proceedings', 'book', 'incollection', 'phdthesis', 'masterthesis', 'www');
CREATE TYPE venue_type AS ENUM ('journal', 'conference', 'book');
CREATE TYPE ref_type AS ENUM ('crossref', 'cite');
CREATE TYPE aff_type AS ENUM('current', 'former');

CREATE SEQUENCE venue_ids START 1;
CREATE SEQUENCE pub_ids START 1;
CREATE SEQUENCE edit_ids START 1;
CREATE SEQUENCE auth_ids START 1;
CREATE SEQUENCE res_ids START 1;
CREATE SEQUENCE authweb_ids START 1;
CREATE SEQUENCE aff_ids START 1;
CREATE SEQUENCE alias_ids START 1;

CREATE TABLE IF NOT EXISTS Venues(
    id INTEGER DEFAULT nextval('venue_ids') PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type venue_type NOT NULL,
    UNIQUE (name, type)
);

CREATE TABLE IF NOT EXISTS Publishers(
    id INTEGER DEFAULT nextval('pub_ids') PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS Editors(
    id INTEGER DEFAULT nextval('pub_ids') PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS Authors(
    key INTEGER DEFAULT nextval('auth_ids') PRIMARY KEY,
    name VARCHAR(255) NOT NUll,
    id VARCHAR(4) NOT NULL,
    mdate DATE,
    UNIQUE(name, id)
);

CREATE TABLE IF NOT EXISTS Publications(
    key VARCHAR(255) PRIMARY KEY,
    mdate DATE NOT NULL,
    title TEXT NOT NULL,
    year int,
    month VARCHAR(255),
    type pub_type NOT NULL,
    school VARCHAR(255),
    isbn VARCHAR(255),
    pages VARCHAR(20),
    volume VARCHAR(255),
    number VARCHAR(255),
    venue_id INT,
    publisher_id INT,
    FOREIGN KEY(venue_id) REFERENCES Venues(id),
    FOREIGN KEY(publisher_id) REFERENCES Publishers(id)
);

CREATE TABLE IF NOT EXISTS Resources(
    id INTEGER DEFAULT nextval('res_ids') PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    value VARCHAR(255) NOT NULL,
    publication_key VARCHAR(255),
    FOREIGN KEY (publication_key) REFERENCES Publications(key)
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

CREATE TABLE IF NOT EXISTS AuthorWebsites(
    key INTEGER DEFAULT nextval('authweb_ids') PRIMARY KEY,
    author_id INT,
    FOREIGN KEY(author_id) REFERENCES Authors(key),
    url VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Affiliations(
    id INTEGER DEFAULT nextval('aff_ids') PRIMARY KEY,
    author_id INT,
    FOREIGN KEY(author_id) REFERENCES Authors(key),
    affiliation VARCHAR(255),
    type aff_type
);

CREATE TABLE IF NOT EXISTS Alias(
    id INTEGER DEFAULT nextval('alias_ids') PRIMARY KEY,
    author_id INT,
    FOREIGN KEY (author_id) REFERENCES Authors(key),
    alias VARCHAR(255)
);