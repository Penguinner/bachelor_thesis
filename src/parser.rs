use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{read_to_string, File};
use std::io::{BufReader, Read};
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
                        b"series" => {publication.references.push(("series".to_string(),self.read_text()?));},
                        b"stream" => {publication.references.push(("stream".to_string(),self.read_text()?));},
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
                let mut a = e.unescape().unwrap().into_owned().to_string();
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
    references: Vec<(String,String)>, // cite, crossref, series, stream
    resources: Vec<(String, String)>, // ee, url, note(without isbn tagged notes)
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
}