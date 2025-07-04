use quick_xml::Reader;
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesStart, Event};
use regex::Regex;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use crate::dblp_sql::{Affiliation, AffiliationType, Alias, Author, AuthorWebsite, Data, DataItem, Editor, Key, PublicationAuthor, PublicationEditor, PublicationKey, PublicationType, Publisher, Reference, RefrenceType, Resource, Venue, VenueType};

pub struct Parser {
    reader: Reader<BufReader<File>>,
}

impl Parser {
    pub fn new(file: &str) -> Parser {
        let file = File::open(file).unwrap();
        let mut reader = Reader::from_reader(BufReader::new(file));
        reader.config_mut().trim_text(true);
        Parser { reader }
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
            return matches!(tag.as_ref(), b"www") & key.starts_with(b"homepage/")
        }
        false
    }

    fn read_publication(&mut self, eve: &BytesStart) -> Result<Option<Record>, Box<dyn Error>> {
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
                        publication.year = self.read_int(e)?;
                    }
                    b"month" => {
                        publication.month = self.read_text(e)?;
                    }
                    b"pages" => {
                        publication.pages = self.read_text(e)?;
                    }
                    b"note" => {
                        let attr = e
                            .try_get_attribute("type");
                        if let Some(attr) = attr.unwrap() { 
                            let value = attr.decode_and_unescape_value(self.reader.decoder())?;
                            match value.as_ref() {
                                "isbn" => {
                                    publication.isbn = self.read_text(e)?;
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
                        publication.number = self.read_text(e)?;
                    }
                    b"volume" => {
                        publication.volume = self.read_text(e)?;
                    }
                    // Article
                    b"journal" => {
                        publication.journal = self.read_text(e)?;
                    }
                    // Proceedings
                    b"publisher" => {
                        publication.publisher = self.read_text(e)?;
                    }
                    b"editor" => {
                        publication.editor.push(self.read_text(e)?);
                    }
                    b"booktitle" => {
                        publication.book_title = self.read_text(e)?;
                    } // Also in inproceedings and incollection
                    // Thesis
                    b"school" => {
                        publication.school = self.read_text(e)?;
                    }
                    // Other
                    b"isbn" => {
                        publication.isbn = self.read_text(e)?;
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
        Ok(Some(Record::Publication(publication)))
    }

    fn read_person(&mut self, eve: &BytesStart) -> Result<Option<Record>, Box<dyn Error>> {
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
        Ok(Some(Record::Person(person)))
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

impl Iterator for Parser {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = Vec::new();
        let mut rec: Option<Record> = None;
        while rec.is_none() {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) if matches!(e.name().as_ref(), b"dblp") => {} // Skip if the tag is dblp
                Ok(Event::Start(e)) if Parser::is_person(&e) => rec = self.read_person(&e).unwrap(),
                Ok(Event::Start(e)) if Parser::is_publication(e.name().as_ref()) => rec = self.read_publication(&e).unwrap(),
                Ok(Event::Eof) => (),
                Err(e) => panic!(
                    "Error at position {}: {:?}",
                    self.reader.buffer_position(),
                    e
                ),
                _ => (),
            }
            buf.clear();
        }
        rec
    }
}

pub enum Record {
    Publication(Publication),
    Person(Person),
}

impl Record {
    pub fn generate_data_items(self) -> Vec<DataItem> {
        match self {
            Record::Publication(publication) => publication.generate_data_items(),
            Record::Person(person) => person.generate_data_items(),
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
    volume: String,
    number: String,
    journal: String,
    publisher: String,
    book_title: String,
    school: String,
    isbn: String,
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
            year: 0,
            month: String::new(),
            pages: String::new(),
            volume: String::new(),
            number: String::new(),
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

    pub fn generate_data_items(self) -> Vec<DataItem> {
        let mut data_items = Vec::new();
        let mut venue_name= String::new();
        let venue_type = match self.pubtype.as_ref() {
            "article" => {
                venue_name = self.journal.clone();
                Some(VenueType::Journal)
            }
            "inproceedings" | "proceedings" => {
                venue_name = self.book_title.clone();
                Some(VenueType::Conference)
            }
            "incollection" => {
                venue_name = self.book_title.clone();
                Some(VenueType::Book)
            }
            _ => None,
        };
        // Venue
        let mut venue_key = None;
        if !venue_name.is_empty() && venue_type.is_some() {
            let mut venue = DataItem::new(
                Data::Venue(
                    Venue {
                        name: venue_name,
                        venue_type: venue_type.unwrap()
                    }
                )
            );
            venue_key = Some(venue.value.key());
            data_items.push(venue);
        }
        // Publisher
        let mut publisher_key = None;
        if !self.publisher.is_empty() {
            let mut publisher = DataItem::new(
                Data::Publisher(Publisher {name: self.publisher})
            );
            publisher_key = Some(publisher.value.key());
            data_items.push(publisher);
        }
        // Publication
        let mut publication = DataItem::new(
            Data::Publication(
                crate::dblp_sql::Publication {
                    key: self.key,
                    mdate: self.mdate,
                    title: self.title,
                    pub_type: PublicationType::from_str(self.pubtype.as_str()).unwrap(),
                    year: match self.year {
                        0 => None,
                        _ => Some(self.year as u32),
                    },
                    month: match self.month.as_str() {
                        "" => None,
                        _ => Some(self.month),
                    },
                    school: match self.school.as_str() {
                        "" => None,
                        _ => Some(self.school),
                    },
                    isbn: match self.isbn.as_str() {
                        "" => None,
                        _ => Some(self.isbn),
                    },
                    pages: match self.pages.as_str() {
                        "" => None,
                        _ => Some(self.pages),
                    },
                    volume: match self.volume.as_str() {
                        "" => None,
                        _ => Some(self.volume),
                    },
                    number: match self.number.as_str() {
                        "" => None,
                        _ => Some(self.number),
                    },
                    venue: match venue_key.clone() {
                        None => None,
                        Some(x) => {
                            match x {
                                Key::Venue(key) => Some(key),
                                _ => panic!("Invalid key"),
                            }
                        }
                    },
                    publisher: match publisher_key.clone() {
                        None => None,
                        Some(x) => {
                            match x {
                                Key::Publisher(key) => Some(key),
                                _ => panic!("Invalid key"),
                            }
                        },
                    }
                }
            )
        );
        let publication_key = publication.value.key();
        if let Some(key) = venue_key {
            publication.add_depends_on(key.clone());
        }
        if let Some(key) = publisher_key {
            publication.add_depends_on(key.clone());
        }
        
        data_items.push(publication);
        // Authors
        for author in &self.authors {
            let author_item = DataItem::new(
              Data::Author(
                  Author {
                      name: author.name.to_string(),
                      id: author.id.to_string(),
                      mdate: author.id.to_string(),
                  }
              )  
            );
            let author_key = author_item.value.key();
            data_items.push(author_item);
            let mut pub_authors = DataItem::new(
                Data::PublicationAuthor(
                    PublicationAuthor {
                        publication: match publication_key.clone() {
                            Key::Publication(key) => key,
                            _ => panic!("Invalid key"),
                        },
                        author: match author_key.clone() {
                                Key::Author(key) => key,
                                _ => panic!("Invalid key"),
                        },
                    }
                )
            );
            pub_authors.add_depends_on(publication_key.clone());
            pub_authors.add_depends_on(author_key.clone());
            data_items.push(pub_authors);
        }
        // Resources
        for resource in &self.resources {
            let mut resource_item = DataItem::new(
                Data::Resource(
                    Resource {
                        resource_type: resource.0.to_string(),
                        value: resource.1.to_string(),
                        publication: match publication_key.clone() {
                            Key::Publication(key) => key,
                            _ => panic!("Invalid key"),
                        },
                    }
                )
            );
            resource_item.add_depends_on(publication_key.clone());
            data_items.push(resource_item);
        }
        // Refrences
        for reference in &self.references {
            let mut reference_item = DataItem::new(
                Data::Reference(
                    Reference {
                        refrence_type: RefrenceType::from_str(reference.0.as_str()).unwrap(),
                        origin: match publication_key.clone() {
                            Key::Publication(key) => key,
                            _ => panic!("Invalid key"),
                        },
                        destination: PublicationKey {
                            key: reference.1.clone()
                        },
                    }
                )
            );
            reference_item.add_depends_on(publication_key.clone());
            reference_item.add_depends_on(Key::Publication(PublicationKey {
                key: reference.1.clone()
            }));
            data_items.push(reference_item);
        }
        // Editors
        for editor in &self.editor {
            let mut editor_item = DataItem::new(
                Data::Editor(
                    Editor {
                        name: editor.clone().to_string(),
                    }
                )
            );
            let editor_key = editor_item.value.key();
            data_items.push(editor_item);
            
            let mut pub_editors =  DataItem::new(
                Data::PublicationEditor(
                    PublicationEditor {
                        publication: match publication_key.clone() {
                            Key::Publication(key) => key,
                            _ => panic!("Invalid key"),
                        },
                        editor: match editor_key.clone() {
                            Key::Editor(key) => key,
                            _ => panic!("Invalid key"),
                        },
                    }
                )
            );
            pub_editors.add_depends_on(publication_key.clone());
            pub_editors.add_depends_on(editor_key.clone());
        }
        data_items
    }
}

pub struct Person {
    name: String,
    id: String,
    alias: Vec<String>,
    mdate: String,
    affiliations: Vec<(String, String)>,
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
        } else {
            self.name = name.to_string();
            self.id = "0001".to_string();
        }
    }

    fn generate_data_items(self) -> Vec<DataItem> {
        let mut data_items = Vec::new();

        // Author
        let author = DataItem::new(
            Data::Author(
                Author {
                    name: self.name.clone().to_string(),
                    id: self.id.clone().to_string(),
                    mdate: self.mdate.clone().to_string(),
                }
            )
        );
        let author_key =  author.value.key();
        data_items.push(author);
        
        // Affiliations
        if !self.affiliations.is_empty() {
            for affiliation in &self.affiliations {
                let mut aff =  DataItem::new(
                    Data::Affiliation(
                        Affiliation {
                            author: match author_key.clone() {
                                Key::Author(key) => key,
                                _ => panic!("Invalid key"),
                            },
                            affiliation: affiliation.0.clone().to_string(),
                            aff_type: AffiliationType::from_str(affiliation.1.as_str()).unwrap(),
                        }
                    )
                );
                aff.add_depends_on(author_key.clone());
                data_items.push(aff);
            }
        }

        // AuthorWebsites
        if !self.urls.is_empty() {
            for url in &self.urls {
                let mut author_website =  DataItem::new(
                    Data::AuthorWebsite(
                        AuthorWebsite {
                            url: url.clone().to_string(),
                            author: match author_key.clone() {
                                Key::Author(key) => key,
                                _ => panic!("Invalid key"),
                            },
                        }
                    )
                );
                author_website.add_depends_on(author_key.clone());
                data_items.push(author_website);
            }
        }

        // Alias
        if !self.alias.is_empty() {
            for alias in &self.alias {
                let mut alias_auth = DataItem::new(
                    Data::Alias(
                        Alias {
                            author: match author_key.clone() {
                                Key::Author(key) => key,
                                _ => panic!("Invalid key"),
                            },
                            alias: alias.to_string(),
                        }
                    )
                );
                alias_auth.add_depends_on(author_key.clone());
                data_items.push(alias_auth);
            }
        }

        data_items
    }
}