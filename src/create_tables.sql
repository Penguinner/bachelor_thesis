CREATE TABLE IF NOT EXISTS Publications(
    key VARCHAR(255) PRIMARY KEY,
    mdate DATE NOT NULL,
    title TEXT NOT NULL,
    year int,
    month TEXT,
    type ENUM('article', 'inproceedings', 'proceedings', 'book', 'incollection', 'phdthesis', 'masterthesis', 'www') NOT NULL,
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
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type ENUM('journal', 'conference', 'book') NOT NULL
);

CREATE TABLE IF NOT EXISTS Resources(
    id INT AUTO_INCREMENT PRIMARY KEY,
    type ENUM('ee', 'url', 'info', 'doi', 'stream', 'series') NOT NULL,
    value VARCHAR(255) NOT NULL,
    publication_key VARCHAR(255),
    FOREIGN KEY (publication_key) REFERENCES Publications(key)
);

CREATE TABLE IF NOT EXISTS Publishers(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS Editors(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS PublicationEditors (
    publication_key VARCHAR(255),
    editor_id INT,
    PRIMARY KEY (publication_key, editor_id),
    FOREIGN KEY (publication_key) REFERENCES Publications(key),
    FOREIGN KEY (editor_id) REFERENCES Editors(id)
)

CREATE TABLE IF NOT EXISTS Reference(
    type ENUM ('crossref', 'cite'),
    origin_pub VARCHAR(255),
    dest_pub VARCHAR(255),
    PRIMARY KEY (origin_publication, dest_publication)
    FOREIGN KEY(origin_publication) REFERENCES Publications(key),
    FOREIGN KEY(dest_publication) REFERENCES Publications(key)
);

CREATE TABLE PublicationAuthors(
    publication_key VARCHAR(255),
    author_id INT,
    PRIMARY KEY (publication_key, author_id),
    FOREIGN KEY (publication_key) REFERENCES Publications(key),
    FOREIGN KEY (author_id) REFERENCES Authors(key)
);

CREATE TABLE IF NOT EXISTS Authors(
    key INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NUll,
    id VARCHAR(4) NOT NULL,
    mdate DATE,
    UNIQUE(name, id)
);

CREATE TABLE IF NOT EXISTS AuthorWebsites(
    key INT AUTO_INCREMENT PRIMARY KEY,
    author_id INT,
    FOREIGN KEY(author_id) REFERENCES Authors(key),
    url VARCHAR(255),
);

CREATE TABLE IF NOT EXISTS Affiliations(
    id INT AUTO_INCREMENT PRIMARY KEY,
    author_id INT,
    FOREIGN KEY(author_id) REFERENCES Authors(key),
    affiliation VARCHAR(255),
    type ENUM('current', 'former')
);

CREATE TABLE IF NOT EXISTS Alias(
    id INT AUTO_INCREMENT PRIMARY KEY,
    author_id INT,
    FOREIGN KEY (author_id) REFERENCES Authors(key),
    alias VARCHAR(255),
);