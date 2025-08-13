use std::collections::HashMap;
use quick_xml::Reader;
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesStart, Event};
use regex::Regex;
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use csv::{Writer, WriterBuilder};
use serde::{Serialize, Serializer};
use serde::ser::SerializeStruct;
use crate::{AFFILIATIONS_FILE, ALIAS_FILE, AUTHOR_FILE, AUTHOR_WEBSITES_FILE, EDITOR_FILE, PUBLICATION_AUTHORS_FILE, PUBLICATION_EDITOR_FILE, PUBLICATION_FILE, PUBLISHER_FILE, REFERENCE_FILE, RESOURCES_FILE, VENUE_FILE};

pub struct Parser {
    reader: Reader<BufReader<File>>,
    next_venue_id: usize,
    next_publisher_id: usize,
    next_editor_id: usize,
    next_author_id: usize,
    next_resource_id: usize,
    next_author_website_id: usize,
    next_affiliation_id: usize,
    next_alias_id: usize,
    venue_map: HashMap<(String, String), usize>,
    publisher_map: HashMap<String, usize>,
    editor_map: HashMap<String, usize>,
    author_map: HashMap<(String, usize), usize>,
    writer: WriteManager,
}

impl Parser {
    pub fn new(file: &str) -> Parser {
        let file = File::open(file).unwrap();
        let mut reader = Reader::from_reader(BufReader::new(file));
        reader.config_mut().trim_text(true);
        Parser { 
            reader,
            next_venue_id: 0,
            next_publisher_id: 0,
            next_editor_id: 0,
            next_author_id: 0,
            next_resource_id: 0,
            next_author_website_id: 0,
            next_affiliation_id: 0,
            next_alias_id: 0,
            venue_map: Default::default(),
            publisher_map: Default::default(),
            editor_map: Default::default(),
            author_map: Default::default(),
            writer: WriteManager::new(),
        }
    }
    
    pub fn run(&mut self) {
        let mut buf = Vec::new();
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) if matches!(e.name().as_ref(), b"dblp") => {} // Skip if the tag is dblp
                Ok(Event::Start(e)) if Parser::is_person(&e) => self.read_person(&e).unwrap(),
                Ok(Event::Start(e)) if Parser::is_publication(e.name().as_ref()) => self.read_publication(&e).unwrap(),
                Ok(Event::Eof) => return self.writer.finalize(),
                Err(e) => panic!(
                    "Error at position {}: {:?}",
                    self.reader.buffer_position(),
                    e
                ),
                _ => (),
            }
            buf.clear();
        }
        
    }

    fn is_publication(tag: &[u8]) -> bool {
        matches!(
            tag,
            b"article"
                | b"inproceedings"
                | b"proceedings"
                | b"book"
                | b"incollection"
                | b"phdthesis"
                | b"masterthesis"
                | b"www"
        )
    }

    fn is_person(e: &BytesStart) -> bool {
        let tag = e.name();
        if let Some(attr) = e.try_get_attribute("key").unwrap() {
            let key = attr.value.as_ref();
            return matches!(tag.as_ref(), b"www") && key.starts_with(b"homepage/")
        }
        false
    }

    fn read_publication(&mut self, eve: &BytesStart) -> Result<(), Box<dyn Error>> {
        let mut buf = Vec::new();
        let mut publication = Publication::new();
        publication.key = String::from(
            eve.try_get_attribute("key")
                .unwrap()
                .unwrap()
                .decode_and_unescape_value(self.reader.decoder())?,
        );
        publication.mdate = String::from(
            eve.try_get_attribute("mdate")
                .unwrap()
                .unwrap()
                .decode_and_unescape_value(self.reader.decoder())?,
        );
        publication.pubtype = match eve.local_name().as_ref() {
            b"article" => "article".to_string(),
            b"inproceedings" => "inproceedings".to_string(),
            b"proceedings" => "proceedings".to_string(),
            b"book" => "book".to_string(),
            b"incollection" => "incollection".to_string(),
            b"phdthesis" => "phdthesis".to_string(),
            b"masterthesis" => "masterthesis".to_string(),
            b"www" => "www".to_string(),
            _ => "".to_string(),
        };
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    //General
                    b"author" => {
                        let author = self.read_text(e)?;
                        let mut person = Person::new();
                        person.add_name(author);
                        publication.authors.push(person);
                    }
                    b"title" => {
                        publication.title = self.read_text(e)?;
                    }
                    b"year" => {
                        publication.year = Some(self.read_int(e)?);
                    }
                    b"month" => {
                        publication.month = Some(self.read_text(e)?);
                    }
                    b"pages" => {
                        publication.pages = Some(self.read_text(e)?);
                    }
                    b"note" => {
                        let attr = e
                            .try_get_attribute("type");
                        if let Some(attr) = attr.unwrap() { 
                            let value = attr.decode_and_unescape_value(self.reader.decoder())?;
                            match value.as_ref() {
                                "isbn" => {
                                    publication.isbn = Some(self.read_text(e)?);
                                }
                                _ => publication
                                    .resources
                                    .push((String::from(value), self.read_text(e)?)),
                            }
                        }
                        else {
                            publication
                                .resources
                                .push((String::from("note"), self.read_text(e)?));
                        }
                        
                    }
                    b"number" => {
                        publication.number = Some(self.read_text(e)?);
                    }
                    b"volume" => {
                        publication.volume = Some(self.read_text(e)?);
                    }
                    // Article
                    b"journal" => {
                        publication.journal = Some(self.read_text(e)?);
                    }
                    // Proceedings
                    b"publisher" => {
                        publication.publisher = Some(self.read_text(e)?);
                    }
                    b"editor" => {
                        publication.editor.push(self.read_text(e)?);
                    }
                    b"booktitle" => {
                        publication.book_title = Some(self.read_text(e)?);
                    } // Also in inproceedings and incollection
                    // Thesis
                    b"school" => {
                        publication.school = Some(self.read_text(e)?);
                    }
                    // Other
                    b"isbn" => {
                        publication.isbn = Some(self.read_text(e)?);
                    }
                    b"cite" | b"crossref"=> {
                        publication
                            .references
                            .push((String::from_utf8_lossy(e.name().as_ref()).into_owned(), self.read_text(e)?));
                    }
                    b"url" | b"ee" | b"series" | b"stream"=> publication
                        .resources
                        .push((String::from_utf8_lossy(e.name().as_ref()).into_owned(), self.read_text(e)?)),
                    _ => {
                        self.reader
                            .read_to_end_into(e.to_end().name(), &mut Vec::new())
                            .unwrap();
                    } // Skip unknown tags
                },
                Ok(Event::End(e)) if e.name().as_ref() == eve.name().as_ref() => break,
                Ok(Event::Eof) => return Err("Unexpected EOF".into()),
                _ => (),
            }
        }
        if !publication.fulfills_constraints() {
            println!("{:?}", publication);
        }
        self.write_publication(publication);
       Ok(())
    }
    
    fn write_publication(&mut self, publication: Publication) {
        // Venue
        let mut venue_name= None;
        let venue_type = match publication.pubtype.as_ref() {
            "article" => {
                venue_name = publication.journal.clone();
                Some("journal".to_string())
            }
            "inproceedings" | "proceedings" => {
                venue_name = publication.book_title.clone();
                Some("conference".to_string())
            }
            "incollection" => {
                venue_name = publication.book_title.clone();
                Some("book".to_string())
            }
            _ => None,
        };
        if venue_name.is_some()
            && venue_type.is_some()
            && !self.venue_map.contains_key(&(venue_name.clone().unwrap(), venue_type.clone().unwrap())) {
            self.venue_map.insert((venue_name.clone().unwrap(), venue_type.clone().unwrap()), self.next_venue_id);
            self.writer.write_venue((self.next_venue_id, venue_name.clone(), venue_type.clone()));
            self.next_venue_id += 1
        }
        // Publisher
        if  publication.publisher.is_some() 
            && !self.publisher_map.contains_key(&publication.publisher.clone().unwrap()) {
            self.publisher_map.insert(publication.publisher.clone().unwrap(), self.next_publisher_id);
            self.writer.write_publisher((self.next_publisher_id, publication.publisher.clone()));
            self.next_publisher_id += 1;
        }
        // Editors
        for editor in publication.editor.iter() {
            if !self.editor_map.contains_key(editor) {
                self.editor_map.insert(editor.clone(), self.next_editor_id);
                self.writer.write_editor((self.next_editor_id, editor.clone()));
                self.next_editor_id += 1;
            }
            self.writer.write_publication_editor((
                publication.key.clone(),
                self.editor_map.get(editor).unwrap().clone(),
            ));
        }
        // Publication
        self.writer.write_publication((
            publication.key.clone(),
            publication.mdate.clone(),
            publication.title.clone(),
            publication.year.clone(),
            publication.month.clone(),
            publication.pubtype.clone(),
            publication.school.clone(),
            publication.isbn.clone(),
            publication.pages.clone(),
            publication.volume.clone(),
            publication.number.clone(),
            self.venue_map.get(&(venue_name.clone().unwrap_or_else(String::new), venue_type.clone().unwrap_or_else(String::new))).copied(),
            self.publisher_map.get(&publication.publisher.clone().unwrap_or_else(String::new)).copied(),
            ));
        // Resources
        for resource in publication.resources.iter() {
            self.writer.write_resource((self.next_resource_id, resource.0.clone(), resource.1.clone(), publication.key.clone()));
            self.next_resource_id += 1;
        }
        // References
        for reference in publication.references.iter() {
            self.writer.write_reference((reference.0.clone(), publication.key.clone(), reference.1.clone() ));
        }
        // Authors
        for author in  publication.authors.iter() {
            if !self.author_map.contains_key(&(author.name.clone(), author.id)) {
                self.author_map.insert((author.name.clone(), author.id), self.next_author_id);
                self.next_author_id += 1;
            }
            self.writer.write_publication_author((
                publication.key.clone(),
                self.author_map.get(&(author.name.clone(), author.id)).unwrap().clone(),
            ));
        }
    }

    fn read_person(&mut self, eve: &BytesStart) -> Result<(), Box<dyn Error>> {
        let mut buf = Vec::new();
        let mut person = Person::new();
        person.mdate = String::from(
            eve.try_get_attribute("mdate")
                .unwrap()
                .unwrap()
                .decode_and_unescape_value(self.reader.decoder())?,
        );
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    b"author" => {
                        let author = self.read_text(e)?;
                        if person.name == String::new() {
                            person.add_name(author);
                        } else {
                            person.alias.push(author);
                        }
                    }
                    b"note" => {
                        let attr = e
                            .try_get_attribute("type")
                            .unwrap()
                            .unwrap()
                            .decode_and_unescape_value(self.reader.decoder())?;
                        if attr == "affiliation" {
                            let state = String::from(
                                e.try_get_attribute("label")
                                    .unwrap_or(Some(Attribute::from(("label", "current"))))
                                    .unwrap()
                                    .decode_and_unescape_value(self.reader.decoder())?,
                            );
                            person.affiliations.push((String::from(attr), state));
                        }
                    }
                    b"url" => {
                        let url = self.read_text(e)?;
                        person.urls.push(url);
                    }
                    _ => {
                        self.reader
                            .read_to_end_into(e.to_end().name(), &mut Vec::new())
                            .unwrap();
                    } // Skip unknown tags
                },
                Ok(Event::End(e)) if e.name().as_ref() == b"www" => break,
                Ok(Event::Eof) => return Err("Unexpected EOF".into()),
                _ => (),
            }
        }
        self.write_person(person);
        Ok(())
    }
    
    fn write_person(&mut self, person: Person) -> () {
        if !self.author_map.contains_key(&(person.name.clone(), person.id)) {
            self.author_map.insert((person.name.clone(), person.id), self.next_author_id);
            self.next_author_id += 1;
        }
        // Author
        self.writer.write_author((
            self.author_map.get(&(person.name.clone(), person.id)).unwrap().clone(),
            person.name.clone(),
            person.id.clone(),
            person.mdate.clone(),
            ));
        // Websites
        for website in person.urls.iter() {
            self.writer.write_author_website((
                self.next_author_website_id,
                self.author_map.get(&(person.name.clone(), person.id)).unwrap().clone(),
                website.clone(),
                ));
            self.next_author_website_id += 1;
        }
        // Affiliations
        for affiliation in person.affiliations.iter() {
            self.writer.write_affiliation((
                self.next_affiliation_id,
                self.author_map.get(&(person.name.clone(), person.id)).unwrap().clone(),
                affiliation.0.clone(),
                affiliation.1.clone(),
                ));
            self.next_affiliation_id += 1;
        }
        // Alias
        for alias in person.alias.iter() {
            self.writer.write_aliases((
                self.next_alias_id,
                self.author_map.get(&(person.name.clone(), person.id)).unwrap().clone(),
                alias.clone(),
                ));
            self.next_alias_id += 1;
        }
    }

    fn read_text(&mut self, start: BytesStart) -> Result<String, Box<dyn Error>> {
        let mut buf = Vec::new();
        let mut text = String::new();
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Text(e)) => {
                    text += e.unescape()?.into_owned().as_str();
                }
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    b"i" | b"ref" | b"sup" | b"sub" | b"tt" => {},
                    _ => return Err(format!("Unexpected event start {0}", String::from_utf8_lossy(e.name().as_ref())).into())
                },
                Ok(Event::End(e)) if e.name().as_ref() == start.name().as_ref() => break,
                Ok(Event::End(e)) => match e.name().as_ref() {
                    b"i" | b"ref" | b"sup" | b"sub" | b"tt" => {},
                    _ => return Err(format!("Unexpected end event {0}", String::from_utf8_lossy(e.name().as_ref())).into()),
                },
                _ => return Err(format!("Unexpected event {0} {1}", self.reader.buffer_position(), self.reader.error_position()).into()),
            }
        }
        Ok(text)
    }
    
    fn read_int(&mut self, start: BytesStart) -> Result<usize, Box<dyn Error>> {
        let value =  self.read_text(start.clone())?;
        Ok(value.parse::<usize>().unwrap_or_else(|e1| { 
            let name = String::from_utf8_lossy(start.name().as_ref()).into_owned();
            panic!("key: {name} value:{value} {e1}")
        }))
    }
}

pub struct Publication {
    pubtype: String,
    key: String,
    mdate: String,
    title: String,
    year: Option<usize>,
    month: Option<String>,
    pages: Option<String>,
    volume: Option<String>,
    number: Option<String>,
    journal: Option<String>,
    publisher: Option<String>,
    book_title: Option<String>,
    school: Option<String>,
    isbn: Option<String>,
    editor: Vec<String>,
    references: Vec<(String, String)>, // cite, crossref
    resources: Vec<(String, String)>,  // ee, url, note (without isbn tagged notes), series, stream
    authors: Vec<Person>,
}

impl Publication {
    fn new() -> Publication {
        Publication {
            pubtype: String::new(),
            key: String::new(),
            mdate: String::new(),
            title: String::new(),
            year: None,
            month: None,
            pages: None,
            volume: None,
            number: None,
            journal: None,
            publisher: None,
            book_title: None,
            school: None,
            isbn: None,
            editor: Vec::new(),
            references: Vec::new(),
            resources: Vec::new(),
            authors: Vec::new(),
        }
    }
    
    pub fn fulfills_constraints(&self) -> bool {
        !self.pubtype.is_empty() && !self.key.is_empty() && !self.mdate.is_empty() && !self.title.is_empty()
    }
}

impl fmt::Debug for Publication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Publication")
            .field("pubtype", &self.pubtype)
            .field("key", &self.key)
            .field("mdate", &self.mdate)
            .field("title", &self.title)
            .field("year", &self.year)
            .field("month", &self.month)
            .field("pages", &self.pages)
            .field("volume", &self.volume)
            .field("number", &self.number)
            .field("journal", &self.journal)
            .field("publisher", &self.publisher)
            .field("book_title", &self.book_title)
            .field("school", &self.school)
            .field("isbn", &self.isbn)
            .field("editor", &self.editor)
            .field("references", &self.references)
            .field("resources", &self.resources)
            .field("authors", &self.authors)
            .finish()
    }
}

pub struct Person {
    name: String,
    id: usize,
    alias: Vec<String>,
    mdate: String,
    affiliations: Vec<(String, String)>,
    urls: Vec<String>,
}

impl Person {
    fn new() -> Person {
        Person {
            name: String::new(),
            id: 1,
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
                self.id = caps[2].parse::<usize>().unwrap();
            });
        } else {
            self.name = name.to_string();
        }
    }
}

impl fmt::Debug for Person {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Person")
            .field("name", &self.name)
            .field("id", &self.id)
            .field("alias", &self.alias)
            .field("mdate", &self.mdate)
            .field("affiliations", &self.affiliations)
            .field("urls", &self.urls)
            .finish()
    }
}

struct WriteManager {
    venues: Vec<(usize, Option<String>, Option<String>)>,
    publishers: Vec<(usize, Option<String>)>,
    editors: Vec<(usize, String)>,
    authors: Vec<(usize, String, usize, String)>,
    publications: Vec<(String, String, String, Option<usize>, Option<String>, String, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<usize>, Option<usize>)>,
    resources: Vec<(usize, String, String, String)>,
    publication_editors: Vec<(String, usize)>,
    references: Vec<(String, String, String)>,
    publication_authors: Vec<(String, usize)>,
    author_websites: Vec<(usize, usize, String)>,
    affiliations: Vec<(usize, usize, String, String)>,
    aliases: Vec<(usize, usize, String)>,
}

impl WriteManager {
    
    pub fn new() -> WriteManager {
        // Touch all csv files and add header
        touch_file(VENUE_FILE, &["id", "name", "type"]);
        touch_file(PUBLISHER_FILE, &["id", "name"]);
        touch_file(EDITOR_FILE, &["id", "name"]);
        touch_file(AUTHOR_FILE, &["key", "id", "name", "mdate"]);
        touch_file(PUBLICATION_FILE,
                   &["key",
                       "mdate",
                       "title",
                       "year",
                       "month",
                       "type",
                       "school",
                       "isbn",
                       "pages",
                       "volume",
                       "number",
                       "venue_id",
                       "publisher_id"]);
        touch_file(RESOURCES_FILE, &["id", "type", "value", "publication_key"]);
        touch_file(PUBLICATION_EDITOR_FILE, &["publication_key", "editor_id"]);
        touch_file(REFERENCE_FILE, &["type", "origin_pub", "dest_pub"]);
        touch_file(PUBLICATION_AUTHORS_FILE, &["publication_key", "author_id"]);
        touch_file(AUTHOR_WEBSITES_FILE, &["id", "author_id", "url"]);
        touch_file(AFFILIATIONS_FILE, &["id", "author_id", "affiliation", "type"]);
        touch_file(ALIAS_FILE, &["id", "author_id", "alias"]);
        WriteManager {
            venues: vec![],
            publishers: vec![],
            editors: vec![],
            authors: vec![],
            publications: vec![],
            resources: vec![],
            publication_editors: vec![],
            references: vec![],
            publication_authors: vec![],
            author_websites: vec![],
            affiliations: vec![],
            aliases: vec![],
        }
    }

    pub fn write_venue(&mut self, tuple: (usize, Option<String>, Option<String>)) {
        self.venues.push(tuple);
        if self.venues.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(VENUE_FILE)
                    .unwrap());
            for tuple in &self.venues {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.venues.clear()
        }
    }
    pub fn write_publisher(&mut self, tuple: (usize, Option<String>)) {
        self.publishers.push(tuple);
        if self.publishers.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(PUBLISHER_FILE)
                    .unwrap());
            for tuple in &self.publishers {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publishers.clear()
        }
    }
    pub fn write_editor(&mut self, tuple: (usize, String)) {
        self.editors.push(tuple);
        if self.editors.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(EDITOR_FILE)
                    .unwrap());
            for tuple in &self.editors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.editors.clear()
        }
    }
    pub fn write_author(&mut self, tuple: (usize, String, usize, String)) {
        self.authors.push(tuple);
        if self.authors.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(AUTHOR_FILE)
                    .unwrap());
            for tuple in &self.authors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.authors.clear()
        }
    }
    pub fn write_publication(&mut self, tuple: (String, String, String, Option<usize>, Option<String>, String, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<usize>, Option<usize>)) {
        self.publications.push(tuple);
        if self.publications.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(PUBLICATION_FILE)
                    .unwrap());
            for tuple in &self.publications {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publications.clear()
        }
    }
    pub fn write_resource(&mut self, tuple: (usize, String, String, String)) {
        self.resources.push(tuple);
        if self.resources.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(RESOURCES_FILE)
                    .unwrap());
            for tuple in &self.resources {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.resources.clear()
        }
    }
    pub fn write_publication_editor(&mut self, tuple: (String, usize)) {
        self.publication_editors.push(tuple);
        if self.publication_editors.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(PUBLICATION_EDITOR_FILE)
                    .unwrap());
            for tuple in &self.publication_editors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publication_editors.clear()
        }
    }
    pub fn write_reference(&mut self, tuple: (String, String, String)) {
        self.references.push(tuple);
        if self.references.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(REFERENCE_FILE)
                    .unwrap());
            for tuple in &self.references {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.references.clear()
        }
    }
    pub fn write_publication_author(&mut self, tuple: (String, usize)) {
        self.publication_authors.push(tuple);
        if self.publication_authors.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(PUBLICATION_AUTHORS_FILE)
                    .unwrap());
            for tuple in &self.publication_authors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publication_authors.clear()
        }
    }
    pub fn write_author_website(&mut self, tuple: (usize, usize, String)) {
        self.author_websites.push(tuple);
        if self.author_websites.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(AUTHOR_WEBSITES_FILE)
                    .unwrap());
            for tuple in &self.author_websites {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.author_websites.clear()
        }
    }
    pub fn write_affiliation(&mut self, tuple: (usize, usize, String, String)) {
        self.affiliations.push(tuple);
        if self.affiliations.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(AFFILIATIONS_FILE)
                    .unwrap());
            for tuple in &self.affiliations {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.affiliations.clear()
        }
    }
    pub fn write_aliases(&mut self, tuple: (usize, usize, String)) {
        self.aliases.push(tuple);
        if self.aliases.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(ALIAS_FILE)
                    .unwrap());
            for tuple in &self.aliases {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.aliases.clear()
        }
    }
    
    pub fn finalize(&mut self) {
        if self.venues.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(VENUE_FILE)
                    .unwrap());
            for tuple in &self.venues {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.venues.clear()
        }
        if self.publishers.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(PUBLISHER_FILE)
                    .unwrap());
            for tuple in &self.publishers {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publishers.clear()
        }
        if self.editors.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(EDITOR_FILE)
                    .unwrap());
            for tuple in &self.editors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.editors.clear()
        }
        if self.affiliations.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(AFFILIATIONS_FILE)
                    .unwrap());
            for tuple in &self.affiliations {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.affiliations.clear()
        }
        if self.authors.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(AUTHOR_FILE)
                    .unwrap());
            for tuple in &self.authors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.authors.clear()
        }
        if self.publications.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(PUBLICATION_FILE)
                    .unwrap());
            for tuple in &self.publications {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publications.clear()
        }
        if self.resources.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(RESOURCES_FILE)
                    .unwrap());
            for tuple in &self.resources {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.resources.clear()
        }
        if self.publication_editors.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(PUBLICATION_EDITOR_FILE)
                    .unwrap());
            for tuple in &self.publication_editors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publication_editors.clear()
        }
        if self.references.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(REFERENCE_FILE)
                    .unwrap());
            for tuple in &self.references {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.references.clear()
        }
        if self.publication_authors.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(PUBLICATION_AUTHORS_FILE)
                    .unwrap());
            for tuple in &self.publication_authors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publication_authors.clear()
        }
        if self.author_websites.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(AUTHOR_WEBSITES_FILE)
                    .unwrap());
            for tuple in &self.author_websites {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.author_websites.clear()
        }
        if self.aliases.len() > 0 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(ALIAS_FILE)
                    .unwrap());
            for tuple in &self.aliases {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.aliases.clear()
        }
    }
}

fn touch_file<I, T>(file: &str, record: I) -> ()
where
    I: IntoIterator<Item=T>,
    T: AsRef<[u8]>,
{
    let mut wrt = WriterBuilder::new().delimiter(b'\t').from_path(file).unwrap();
    wrt.write_record(record).unwrap();
    wrt.flush().unwrap();
}