use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;
use regex::Regex;
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{read_to_string, File};
use std::io::BufReader;
use phf::phf_map;

pub static ENTITY_MAP: phf::Map<&'static str, &'static str> = phf_map! {
    "Agrave" => "\u{00C0}",  // À
    "Aacute" => "\u{00C1}",  // Á
    "Acirc" => "\u{00C2}",   // Â
    "Atilde" => "\u{00C3}",  // Ã
    "Auml" => "\u{00C4}",    // Ä
    "Aring" => "\u{00C5}",   // Å
    "AElig" => "\u{00C6}",   // Æ
    "Ccedil" => "\u{00C7}",  // Ç
    "Egrave" => "\u{00C8}",  // È
    "Eacute" => "\u{00C9}",  // É
    "Ecirc" => "\u{00CA}",   // Ê
    "Euml" => "\u{00CB}",    // Ë
    "Igrave" => "\u{00CC}",  // Ì
    "Iacute" => "\u{00CD}",  // Í
    "Icirc" => "\u{00CE}",   // Î
    "Iuml" => "\u{00CF}",    // Ï
    "ETH" => "\u{00D0}",     // Ð
    "Ntilde" => "\u{00D1}",  // Ñ
    "Ograve" => "\u{00D2}",  // Ò
    "Oacute" => "\u{00D3}",  // Ó
    "Ocirc" => "\u{00D4}",   // Ô
    "Otilde" => "\u{00D5}",  // Õ
    "Ouml" => "\u{00D6}",    // Ö
    "Oslash" => "\u{00D8}",  // Ø
    "Ugrave" => "\u{00D9}",  // Ù
    "Uacute" => "\u{00DA}",  // Ú
    "Ucirc" => "\u{00DB}",   // Û
    "Uuml" => "\u{00DC}",    // Ü
    "Yacute" => "\u{00DD}",  // Ý
    "THORN" => "\u{00DE}",   // Þ
    "szlig" => "\u{00DF}",   // ß
    "agrave" => "\u{00E0}",  // à
    "aacute" => "\u{00E1}",  // á
    "acirc" => "\u{00E2}",   // â
    "atilde" => "\u{00E3}",  // ã
    "auml" => "\u{00E4}",    // ä
    "aring" => "\u{00E5}",   // å
    "aelig" => "\u{00E6}",   // æ
    "ccedil" => "\u{00E7}",  // ç
    "egrave" => "\u{00E8}",  // è
    "eacute" => "\u{00E9}",  // é
    "ecirc" => "\u{00EA}",   // ê
    "euml" => "\u{00EB}",    // ë
    "igrave" => "\u{00EC}",  // ì
    "iacute" => "\u{00ED}",  // í
    "icirc" => "\u{00EE}",   // î
    "iuml" => "\u{00EF}",    // ï
    "eth" => "\u{00F0}",     // ð
    "ntilde" => "\u{00F1}",  // ñ
    "ograve" => "\u{00F2}",  // ò
    "oacute" => "\u{00F3}",  // ó
    "ocirc" => "\u{00F4}",   // ô
    "otilde" => "\u{00F5}",  // õ
    "ouml" => "\u{00F6}",    // ö
    "oslash" => "\u{00F8}",  // ø
    "ugrave" => "\u{00F9}",  // ù
    "uacute" => "\u{00FA}",  // ú
    "ucirc" => "\u{00FB}",   // û
    "uuml" => "\u{00FC}",    // ü
    "yacute" => "\u{00FD}",  // ý
    "thorn" => "\u{00FE}",   // þ
    "yuml" => "\u{00FF}",    // ÿ
};

pub struct Parser {
    reader: Reader<BufReader<File>>,
}

impl Parser {
    pub fn new(file: &str) -> Parser {
        let file = File::open(file).unwrap();
        let mut reader = Reader::from_reader(BufReader::new(file));
        reader.config_mut().trim_text(true);
        Parser { reader}
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
        publication.key = String::from(eve.try_get_attribute("key").unwrap().unwrap().decode_and_unescape_value(self.reader.decoder())?);
        publication.mdate = String::from(eve.try_get_attribute("mdate").unwrap().unwrap().decode_and_unescape_value(self.reader.decoder())?);
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
                            let attr = e.try_get_attribute("type").unwrap().unwrap().decode_and_unescape_value(self.reader.decoder())?;
                            match attr.as_ref() {
                                "isbn" => {publication.isbn = self.read_text()?.parse()?;},
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
        person.mdate = String::from(eve.try_get_attribute("mdate").unwrap().unwrap().decode_and_unescape_value(self.reader.decoder())?);
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
                            let attr = e.try_get_attribute("type").unwrap().unwrap().decode_and_unescape_value(self.reader.decoder())?;
                            if attr == "affiliation" {
                                let state = String::from(e.try_get_attribute("label")
                                    .unwrap_or(Some(Attribute::from(("label","current"))))
                                    .unwrap()
                                    .decode_and_unescape_value(self.reader.decoder())?);
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
        Ok(Some(Record::Person(person)))
    }

    fn read_text(&mut self) -> Result<String, Box<dyn Error>> {
        let mut buf = Vec::new();
        match self.reader.read_event_into(&mut buf) {
            Ok(Event::Text(e)) => {
                let a = e.unescape()?.into_owned();
                let re = Regex::new(r"&(\w+);").unwrap();
                let result = re.replace_all(&a, |caps: &regex::Captures| {
                    let key = &caps[1];
                    match ENTITY_MAP.get(key) {
                        Some(&char_str) => Cow::Borrowed(char_str),
                        None => Cow::Owned(caps[0].to_string()),
                    }
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
                if Parser::is_person(e.name().as_ref(), e.try_get_attribute("key").unwrap().unwrap().value.as_ref()) => {
                    rec = self.read_person(&e).unwrap();
                },
                Ok(Event::Start(e))
                if Parser::is_publication(e.name().as_ref()) => {
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
    pub fn generate_sql_ops(&self) -> (Vec<String>,Vec<String>) {
        match self {
            Record::Publication(publication) => publication.to_owned().generate_sql_ops(),
            Record::Person(person) => (person.to_owned().generate_sql_ops(), Vec::new()),
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
    resources: Vec<(String, String)>, // ee, url, note (without isbn tagged notes), series, stream
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

    pub fn generate_sql_ops(&self) -> (Vec<String>,Vec<String>) {
        let mut sql_ops = Vec::new();
        let mut ref_sql_ops = Vec::new(); 
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
        // Venue
        if !venue_name.is_empty() && !venue_type.is_empty() {
            sql_ops.push(
                format!(
                    "INSERT INTO Venues (name, type) VALUES('{0}', '{1}') ON CONFLICT DO NOTHING;",
                venue_name, venue_type)
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
            ref_sql_ops.push(
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
        (sql_ops,ref_sql_ops)
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

    fn generate_sql_ops(&self) -> Vec<String> {
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