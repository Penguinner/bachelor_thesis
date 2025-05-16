use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{read_to_string, File};
use std::io::BufReader;
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use quick_xml::events::attributes::Attribute;
use regex::Regex;

pub struct Parser {
    reader: Reader<BufReader<File>>,
    replacements: HashMap<String, String>,
}

impl Parser {
    pub fn new(file: &str) -> Parser {
        let file = File::open(file).unwrap();
        let mut reader = Reader::from_reader(BufReader::new(file));
        reader.config_mut().trim_text(true);
        let mut replacements = HashMap::new();
        let content = read_to_string("src/replacements.txt").unwrap();
        for line in content.lines() {
            let splits: Vec<&str> = line.split_whitespace().collect();
            replacements.insert(splits[0].to_owned(), char::from_u32(splits[1].parse::<u32>().unwrap()).unwrap().to_string());
        }
        Parser { reader, replacements }
    }

    fn is_publication(tag: &[u8]) -> bool {
        matches!(
        tag,
        b"article" |
        b"inproceedings" |
        b"proceedings" |
        b"book" |
        b"incollection" |
        b"phdthesis" |
        b"masterthesis" |
        b"www"
    )
    }

    fn is_person(tag: &[u8], key: &[u8]) -> bool {
        matches!(tag,b"www") & key.starts_with(b"homepage/")
    }

    fn read_publication(&mut self, eve: &BytesStart) -> Result<Option<Record>, Box<dyn Error>> {
        let mut buf = Vec::new();
        let mut publication = Publication::new();
        publication.key = String::from(eve.try_get_attribute("key").unwrap().unwrap().value);
        publication.mdate = String::from(eve.try_get_attribute("mdate").unwrap().unwrap().value);
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) =>
                    match e.name().as_ref() {
                        //General
                        b"author" => {
                            let author = self.read_text()?;
                            let mut person = Person::new();
                            person.add_name(author);
                            publication.authors.push(person);
                        },
                        b"title" => {publication.title = self.read_text()?;},
                        b"year" => {publication.year = self.read_text()?.parse()?;},
                        b"month" => {publication.month = self.read_text()?;},
                        b"pages" => {publication.pages = self.read_text()?.parse()?;},
                        b"url" => {publication.resources.push(("url".to_string(),self.read_text()?))},
                        b"ee" => {publication.resources.push(("ee".to_string(),self.read_text()?))},
                        b"note" => {
                            let attr = e.try_get_attribute("type").unwrap().unwrap().value.as_ref();
                            match attr {
                                b"isbn" => {publication.isbn = self.read_text()?.parse()?;},
                                _ => {publication.resources.push((String::from(attr),self.read_text()?))},
                            }
                        },
                        b"number" => {publication.number = self.read_text()?.parse()?;},
                        b"volume" => {publication.volume = self.read_text()?.parse()?;},
                        // Article
                        b"journal" => {publication.journal = self.read_text()?;},
                        // Proceedings
                        b"publisher" => {publication.publisher = self.read_text()?;},
                        b"editor" => {publication.editor.push(self.read_text()?);},
                        b"booktitle" => {publication.book_title = self.read_text()?;}, // Also in inproceedings and incollection
                        // Thesis
                        b"school" => {publication.school = self.read_text()?;},
                        // Other
                        b"isbn" => {publication.isbn = self.read_text()?;},
                        b"cite" => {publication.references.push(("cite".to_string(),self.read_text()?));},
                        b"crossref" => {publication.references.push(("crossref".to_string(),self.read_text()?));},
                        b"series" => {publication.resources.push(("series".to_string(),self.read_text()?));},
                        b"stream" => {publication.resources.push(("stream".to_string(),self.read_text()?));},
                        _ => { self.reader.read_to_end_into(e.to_end().name(), &mut Vec::new()).unwrap();} // Skip unknown tags
                    },
                Ok(Event::End(e)) if e.name().as_ref() == eve.name().as_ref() => break,
                Ok(Event::Eof) => return Err("Unexpected EOF".into()),
                _ => (),
            }
        }
        Ok(Some(Record::Publication(publication)))
    }

    fn read_person(&mut self, eve: &BytesStart) -> Result<Option<Record>, Box<dyn Error>> {
        let mut buf = Vec::new();
        let mut person = Person::new();
        person.mdate = String::from(eve.try_get_attribute("mdate").unwrap().unwrap().value);
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) =>
                    match e.name().as_ref() {
                        b"author" => {
                            let author = self.read_text()?;
                            if person.name == String::new() {
                                person.add_name(author);
                            }
                            else {
                                person.alias.push(author);
                            }
                        },
                        b"note" => {
                            let attr = e.try_get_attribute("type").unwrap().unwrap().value.as_ref();
                            if attr == b"affiliation" {
                                let state = String::from(e.try_get_attribute("label")
                                    .unwrap_or(Some(Attribute::from("current")))
                                    .unwrap()
                                    .value
                                    .as_ref());
                                person.affiliations.push((String::from(attr),state));
                            }
                        },
                        b"url" => {
                            let url = self.read_text()?;
                            person.urls.push(url);
                        },
                        _ => { self.reader.read_to_end_into(e.to_end().name(), &mut Vec::new()).unwrap();} // Skip unknown tags
                    }
                Ok(Event::End(e)) if e.name().as_ref() == b"www" => break,
                Ok(Event::Eof) => return Err("Unexpected EOF".into()),
                _ => (),
            }
        }
        Ok(None)
    }

    fn read_text(&mut self) -> Result<String, Box<dyn Error>> {
        let mut buf = Vec::new();
        match self.reader.read_event_into(&mut buf) {
            Ok(Event::Text(e)) => {
                let a = e.unescape().unwrap().into_owned().to_string();
                let re = Regex::new(r"&(\w+);").unwrap();
                let result = re.replace_all(a.as_str(), |caps: &regex::Captures| {
                    let key = &caps[1];
                    self.replacements.get(key)
                        .map(|&val| Cow::Borrowed(val.as_str()))
                        .unwrap_or_else(|| Cow::Borrowed( &caps[0]))
                });
                Ok(result.to_string())
            }
            _ => Err("Unexpected tag".into()),
        }
    }
}

impl Iterator for Parser {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = Vec::new();
        let mut rec: Option<Record> = None;
        while rec.is_none() {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e))
                if matches!(e.name().as_ref(), b"dblp")=> {}, // Skip if the tag is dblp
                Ok(Event::Start(e))
                if self.is_person(e.name().as_ref(), e.try_get_attribute("key").unwrap().unwrap().value.as_ref()) => {
                    rec = self.read_person(&e).unwrap();
                },
                Ok(Event::Start(e))
                if self.is_publication(e.name().as_ref()) => {
                    rec = self.read_publication( &e).unwrap();
                },
                Ok(Event::Eof) => (),
                Err(e) => panic!("Error at position {}: {:?}", self.reader.buffer_position(), e),
                _ => (),
            }
            buf.clear();
        }
        rec
    }
}

pub enum Record {
    Publication(Publication),
    Person(Person)
}

impl Record {
    pub fn generate_sql_ops(&self) -> Vec<String> {
        match self {
            Record::Publication(publication) => publication.to_owned().generate_sql_ops(),
            Record::Person(person) => person.to_owned().generate_sql_ops(),
        }
    }
}

pub struct Publication {
    pubtype: String,
    key: String,
    mdate: String,
    title: String,
    year: usize,
    month: String,
    pages: String,
    volume: usize,
    number: usize,
    journal: String,
    publisher: String,
    book_title: String,
    school: String,
    isbn: String,
    editor: Vec<String>,
    references: Vec<(String,String)>, // cite, crossref
    resources: Vec<(String, String)>, // ee, url, note(without isbn tagged notes), series, stream
    authors: Vec<Person>,
}

impl Publication {

    fn new() -> Publication {
        Publication {
            pubtype: String::new(),
            key: String::new(),
            mdate: String::new(),
            title: String::new(),
            year: 0,
            month: String::new(),
            pages: String::new(),
            volume: 0,
            number: 0,
            journal: String::new(),
            publisher: String::new(),
            book_title: String::new(),
            school: String::new(),
            isbn: String::new(),
            editor: Vec::new(),
            references: Vec::new(),
            resources: Vec::new(),
            authors: Vec::new(),
        }
    }

    pub fn generate_sql_ops(&mut self) -> Vec<String> {
        let mut sql_ops = Vec::new();
        // Venues
        let mut venue_name = String::new();
        let mut venue_type = String::new();
        match self.pubtype.as_ref() {
            "article" => {
                venue_name = self.journal.clone();
                venue_type = "journal".to_string();
            },
            "inproceedings" | "proceedings" => {
                venue_name = self.book_title.clone();
                venue_type = "conference".to_string();
            }
            "incollection" => {
                venue_name = self.book_title.clone();
                venue_type = "book".to_string();
            }
            _ => ()
        };
        if !venue_name.is_empty() && !venue_type.is_empty() {
            sql_ops.push(
                format!(
                    "INSERT INTO Venues (name, type) VALUES('{0}', '{1}');",
                venue_name, venue_type)
            );
        }
        // Authors
        for author in &self.authors {
            sql_ops.push(
                format!(
                    "INSERT INTO Authors (name, id) VALUES ('{0}', '{1}');",
                    author.name, author.id
                )
            );
            sql_ops.push(
                format!(
                    "INSERT INTO PublicationAuthors (publication_key, author_id) VALUES ('{0}', '{1}') ON CONFLICT DO NOTHING;",
                    self.key,
                    format!(
                        "SELECT id FROM WHERE name = '{0}' AND id = '{1}'",
                        author.name, author.id
                    )
                )
            );
        }
        // Resources
        for resource in &self.resources {
            sql_ops.push(
              format!(
                  "INSERT INTO Resources (type, value, publication_key) VALUES ('{0}', '{1}', '{2}');",
                  resource.0, resource.1, self.key
              )
            );
        }
        // Refrences
        for reference in &self.references {
            sql_ops.push(
                format!(
                    "INSERT INTO Reference (type, origin_pub, dest_pub) VALUES ('{0}', '{1}', '{2}');",
                    reference.0, self.key, reference.1
                )
            );
        }
        // Editors
        for editor in &self.editor {
            sql_ops.push(
                format!(
                    "INSERT INTO Editors (name) VALUES ('{0}') ON CONFLICT DO NOTHING;",
                    editor
                )
            );
            sql_ops.push(
                format!(
                    "INSERT INTO PublicationEditors (publication_key, editor_id) VALUES ('{0}', '{1}') ON CONFLICT DO NOTHING;",
                    self.key,
                    format!("(SELECT id FROM Editors WHERE name = '{}')", editor),
                )
            );
        }
        // Publisher
        if !self.publisher.is_empty() {
            sql_ops.push(
                format!(
                    "INSERT INTO Publishers (name) VALUES ('{0}') ON CONFLICT DO NOTHING;", self.publisher
                )
            );
        }
        // Publication
        let mut extra_keys = String::new();
        let mut extra_values = String::new();
        if self.year != 0 {
            extra_keys.push_str(", year");
            extra_values.push_str(format!(", {}", self.year).as_str());
        }
        if !self.month.is_empty() {
            extra_keys.push_str(", month");
            extra_values.push_str(format!(", {}", self.month).as_str());
        }
        if !self.school.is_empty() {
            extra_keys.push_str(", school");
            extra_values.push_str(format!(", {}", self.school).as_str());
        }
        if !self.isbn.is_empty() {
            extra_keys.push_str(", isbn");
            extra_values.push_str(format!(", {}", self.isbn).as_str());
        }
        if !self.pages.is_empty() {
            extra_keys.push_str(", pages");
            extra_values.push_str(format!(", {}", self.pages).as_str());
        }
        if self.volume != 0 {
            extra_keys.push_str(", volume");
            extra_values.push_str(format!(", {}", self.volume).as_str());
        }
        if self.number != 0 {
            extra_keys.push_str(", number");
            extra_values.push_str(format!(", {}", self.number).as_str());
        }
        if !venue_name.is_empty() && !venue_type.is_empty() {
            extra_keys.push_str(", venue_id");
            extra_values.push_str(
                format!(
                    ", (SELECT id FROM Venues WHERE name='{0}' AND type='{1}')",
                    venue_name, venue_type
                ).as_str());
        }
        if !self.publisher.is_empty() {
            extra_keys.push_str(", publisher");
            extra_values.push_str(
                format!(
                    ", (SELECT id FROM Publishers WHERE name='{0}')",
                    self.publisher
                ).as_str()
            )
        }

        sql_ops.push(
            format!(
                "INSERT INTO Publications (key, mdate, title, type{0}) VALUES ('{1}', '{2}', '{3}', '{4}'{5}) ON CONFLICT DO NOTHING;",
                extra_keys,
                self.key,
                self.mdate,
                self.title,
                self.pubtype,
                extra_values
            )
        );
        sql_ops
    }
}

pub struct Person {
    name: String,
    id: String,
    alias: Vec<String>,
    mdate: String,
    affiliations: Vec<(String,String)>,
    urls: Vec<String>,
}

impl Person {
    fn new() -> Person {
        Person {
            name: String::new(),
            id: String::new(),
            alias: Vec::new(),
            mdate: String::new(),
            affiliations: Vec::new(),
            urls: Vec::new(),
        }
    }

    fn add_name(&mut self, name: String) {
        let re = Regex::new(r"(.*)\s+(\d+)").unwrap();
        let name = name.trim();
        if re.is_match(&name) {
            re.captures(name).map(|caps| {
                self.name = caps[1].to_string();
                self.id = caps[2].to_string();
            });
        }
        else {
            self.name = name.to_string();
            self.id = "0001".to_string();
        }
    }

    fn generate_sql_ops(&mut self) -> Vec<String> {
        let mut sql_ops = Vec::new();

        // Author
        sql_ops.push(
            format!(
                "INSERT INTO Authors(name, id, mdate) VALUES ('{0}', '{1}','{2}') ON CONFLICT DO UPDATE \
                SET mdate = excluded.mdate;",
                self.name,
                self.id,
                self.mdate
            )
        );

        // Affiliations
        if !self.affiliations.is_empty() {
            for affiliation in &self.affiliations {
                sql_ops.push(
                    format!(
                        "INSERT INTO Affiliations( author_id, affiliation, type)\
                         VALUES ( \
                         (SELECT key FROM Authors WHERE name='{0}' AND id='{1}'),\
                         '{2}', '{3}');",
                        self.name,self.id,affiliation.0,affiliation.1
                    )
                )
            }
        }

        // AuthorWebsites
        if !self.urls.is_empty() {
            for url in &self.urls {
                sql_ops.push(
                    format!(
                        "INSERT INTO AuthorWebsites (author_id, url) VALUES ( \
                    (SELECT key FROM Authors WHERE name='{0}' AND id='{1}'),\
                    '{2}');",
                        self.name, self.id, url
                    )
                )
            }
        }

        // Alias
        if !self.alias.is_empty() {
            for alias in &self.alias {
                sql_ops.push(
                    format!(
                        "INSERT INTO Alias (author_id, alias) VALUES ( \
                    (SELECT key FROM Authors WHERE name='{0}' AND id='{1}'),\
                    '{2}');",
                        self.name, self.id, alias
                    )
                )
            }
        }

        sql_ops
    }
}