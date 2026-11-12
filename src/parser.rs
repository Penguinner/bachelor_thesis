use crate::{
    AFFILIATIONS_FILE, ALIAS_FILE, AUTHOR_FILE, AUTHOR_WEBSITES_FILE, EDITOR_FILE,
    PUBLICATION_AUTHORS_FILE, PUBLICATION_EDITOR_FILE, PUBLICATION_FILE, PUBLISHER_FILE,
    REFERENCE_FILE, RESOURCES_FILE, VENUE_FILE,
};
use csv::WriterBuilder;
use quick_xml::Reader;
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesStart, Event};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::BufReader;

/// Parser for parsing the DBLP xml data into a SQL structure
pub struct Parser {
    reader: Reader<BufReader<File>>,
    next_publication_id: usize,
    next_venue_id: usize,
    next_publisher_id: usize,
    next_editor_id: usize,
    next_author_id: usize,
    next_resource_id: usize,
    next_author_website_id: usize,
    next_affiliation_id: usize,
    next_alias_id: usize,
    publication_map: HashMap<String, usize>,
    venue_map: HashMap<(String, String), usize>,
    publisher_map: HashMap<String, usize>,
    editor_map: HashMap<String, usize>,
    author_map: HashMap<(String, usize), usize>,
    writer: WriteManager,
    publications: Vec<Publication>,
}

impl Parser {
    /// Create a new Parser for a file.
    pub fn new(file: &str) -> Parser {
        let file = File::open(file).unwrap();
        let mut reader = Reader::from_reader(BufReader::new(file));
        reader.config_mut().trim_text(true);
        Parser {
            reader,
            next_publication_id: 0,
            next_venue_id: 0,
            next_publisher_id: 0,
            next_editor_id: 0,
            next_author_id: 0,
            next_resource_id: 0,
            next_author_website_id: 0,
            next_affiliation_id: 0,
            next_alias_id: 0,
            publication_map: Default::default(),
            venue_map: Default::default(),
            publisher_map: Default::default(),
            editor_map: Default::default(),
            author_map: Default::default(),
            writer: WriteManager::new(),
            publications: vec![],
        }
    }

    // Start the parser
    pub fn run(&mut self) {
        let mut buf = Vec::new();
        // Read xml events
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) if matches!(e.name().as_ref(), b"dblp") => {} // Skip if the tag is dblp
                Ok(Event::Start(e)) if Parser::is_person(&e) => self.read_person(&e).unwrap(),
                Ok(Event::Start(e)) if Parser::is_publication(e.name().as_ref()) => {
                    self.read_publication(&e).unwrap()
                }
                Ok(Event::Eof) => break,
                Err(e) => panic!(
                    "Error at position {}: {:?}",
                    self.reader.buffer_position(),
                    e
                ),
                _ => (),
            }
            buf.clear();
        }
        self.writer.finalize();
        self.write_publications();
        self.writer.finalize();
    }

    /// Checks if a given xml tag is a publication.
    /// Accepts "\<article>", "<inproceedings>", "\<proceedings>", "\<book>", "\<incollection>", "\<phdthesis>", "\<masterthesis>" and "\<www>"
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

    /// Checks if a given xml tag is a person record.
    /// Defined by dblp as \<www key="homepages/...>".
    fn is_person(e: &BytesStart) -> bool {
        let tag = e.name();
        if let Some(attr) = e.try_get_attribute("key").unwrap() {
            let key = attr.value.as_ref();
            return matches!(tag.as_ref(), b"www") && key.starts_with(b"homepages/");
        }
        false
    }

    /// Read the current xml tag until the end into a publication
    fn read_publication(&mut self, eve: &BytesStart) -> Result<(), Box<dyn Error>> {
        let mut buf = Vec::new();
        let mut publication = Publication::new();
        // Get Publication key, Modification Date and Bibtex/Publication type
        let re_pubkey = Regex::new(r"\S+/\S+/\S+").unwrap();
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

        // Give publication an id if it doesn't already exist
        if !self.publication_map.contains_key(&publication.key) {
            self.publication_map
                .insert(publication.key.clone(), self.next_publication_id);
            self.next_publication_id += 1;
        }
        // Continue to read nested xml tags
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    //General
                    b"author" => {
                        let author = self.read_text(&e)?;
                        let mut person = Person::new();
                        person.add_name(author.as_str());
                        // Insert author into publication
                        if !publication.authors.contains(&person) {
                            publication.authors.push(person);
                        }
                    }
                    b"title" => {
                        publication.title = self.read_text(&e)?;
                    }
                    b"year" => {
                        publication.year = Some(self.read_int(&e)?);
                    }
                    b"month" => {
                        publication.month = Some(self.read_text(&e)?);
                    }
                    b"pages" => {
                        publication.pages = Some(self.read_text(&e)?);
                    }
                    b"note" => {
                        let attr = e.try_get_attribute("type");
                        if let Some(attr) = attr.unwrap() {
                            let value = attr.decode_and_unescape_value(self.reader.decoder())?;
                            match value.as_ref() {
                                "isbn" => {
                                    // Notes with type isbn are saved as isbn
                                    publication.isbn = Some(self.read_text(&e)?);
                                }
                                _ => publication // Notes with other type are saved as resources
                                    .resources
                                    .push((String::from(value), self.read_text(&e)?)),
                            }
                        } else {
                            // Type less notes are saved as notes
                            publication
                                .resources
                                .push((String::from("note"), self.read_text(&e)?));
                        }
                    }
                    b"number" => {
                        publication.number = Some(self.read_text(&e)?);
                    }
                    b"volume" => {
                        publication.volume = Some(self.read_text(&e)?);
                    }
                    // Article
                    b"journal" => {
                        publication.journal = Some(self.read_text(&e)?);
                    }
                    // Proceedings
                    b"publisher" => {
                        publication.publisher = Some(self.read_text(&e)?);
                    }
                    b"editor" => {
                        publication.editor.push(self.read_text(&e)?);
                    }
                    b"booktitle" => {
                        publication.book_title = Some(self.read_text(&e)?);
                    } // Also in inproceedings and incollection
                    // Thesis
                    b"school" => {
                        publication.school = Some(self.read_text(&e)?);
                    }
                    // Other
                    b"isbn" => {
                        publication.isbn = Some(self.read_text(&e)?);
                    }
                    b"cite" | b"crossref" => {
                        let text = self.read_text(&e)?;
                        if !text.contains("homepages/") && re_pubkey.is_match(&text) {
                            // Filter out homepage references and not proper refrences
                            let ref_id = match self.publication_map.get(&text) {
                                Some(v) => *v,
                                None => {
                                    self.publication_map.insert(text, self.next_publication_id);
                                    self.next_publication_id += 1;
                                    self.next_publication_id - 1
                                }
                            };

                            let reference = (
                                String::from_utf8_lossy(e.name().as_ref()).into_owned(),
                                ref_id,
                            );
                            if !publication.references.contains(&reference) {
                                publication.references.push(reference);
                            }
                        }
                    }
                    b"url" | b"ee" | b"series" | b"stream" => publication.resources.push((
                        String::from_utf8_lossy(e.name().as_ref()).into_owned(),
                        self.read_text(&e)?,
                    )),
                    _ => {
                        self.reader
                            .read_to_end_into(e.to_end().name(), &mut Vec::new())
                            .unwrap();
                    } // Skip unknown tags
                },
                Ok(Event::End(e)) if e.name().as_ref() == eve.name().as_ref() => break, // If end of initial publication tag is reached break
                Ok(Event::Eof) => return Err("Unexpected EOF".into()),
                _ => (),
            }
        }
        // Check if publication fulfills standards
        if !publication.fulfills_constraints() {
            println!("{:?}", publication);
            println!("{:?}", eve);
        } else {
            self.publications.push(publication);
        }
        Ok(())
    }

    /// Write publication to csv file
    fn write_publications(&mut self) {
        for publication in self.publications.iter_mut() {
            // Venue
            let pub_id = match self.publication_map.get(&publication.key) {
                Some(v) => *v,
                None => {
                    self.publication_map
                        .insert(publication.key.clone(), self.next_publication_id);
                    self.next_publication_id += 1;
                    self.next_publication_id - 1
                }
            };
            let mut venue_name = None;
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
                && !self
                    .venue_map
                    .contains_key(&(venue_name.clone().unwrap(), venue_type.clone().unwrap()))
            {
                self.venue_map.insert(
                    (venue_name.clone().unwrap(), venue_type.clone().unwrap()),
                    self.next_venue_id,
                );
                self.writer.write_venue(VenueStore {
                    id: self.next_venue_id,
                    name: venue_name.clone(),
                    venue_type: venue_type.clone(),
                });
                self.next_venue_id += 1
            }
            // Publisher
            if publication.publisher.is_some()
                && !self
                    .publisher_map
                    .contains_key(&publication.publisher.clone().unwrap())
            {
                self.publisher_map.insert(
                    publication.publisher.clone().unwrap(),
                    self.next_publisher_id,
                );
                self.writer.write_publisher(PublisherStore {
                    id: self.next_publisher_id,
                    name: publication.publisher.clone(),
                });
                self.next_publisher_id += 1;
            }
            // Editors
            for editor in publication.editor.iter() {
                if !self.editor_map.contains_key(editor) {
                    self.editor_map.insert(editor.clone(), self.next_editor_id);
                    self.writer.write_editor(EditorStore {
                        id: self.next_editor_id,
                        name: editor.clone(),
                    });
                    self.next_editor_id += 1;
                }
                self.writer
                    .write_publication_editor(PublicationEditorStore {
                        publication_id: pub_id,
                        editor_id: *self.editor_map.get(editor).unwrap(),
                    });
            }
            // Publication
            let venue_id = self
                .venue_map
                .get(&(
                    venue_name.clone().unwrap_or_else(String::new),
                    venue_type.clone().unwrap_or_else(String::new),
                ))
                .copied();

            let publisher_id = self
                .publisher_map
                .get(&publication.publisher.clone().unwrap_or_else(String::new))
                .copied();

            self.writer.write_publication(PublicationStore {
                id: pub_id,
                key: publication.key.clone(),
                mdate: publication.mdate.clone(),
                title: publication.title.clone(),
                year: publication.year,
                month: publication.month.clone(),
                pub_type: publication.pubtype.clone(),
                school: publication.school.clone(),
                isbn: publication.isbn.clone(),
                pages: publication.pages.clone(),
                volume: publication.volume.clone(),
                number: publication.number.clone(),
                venue_id,
                publisher_id,
            });
            // Resources
            for resource in publication.resources.iter() {
                self.writer.write_resource(ResourceStore {
                    id: self.next_resource_id,
                    res_type: resource.0.clone(),
                    value: resource.1.clone(),
                    publication_id: pub_id,
                });
                self.next_resource_id += 1;
            }
            // References
            for reference in publication.references.iter() {
                self.writer.write_reference(ReferenceStore {
                    ref_type: reference.0.clone(),
                    origin_pub: pub_id,
                    dest_pub: reference.1,
                });
            }
            // Authors
            let mut auth_ids_present: Vec<usize> = Vec::new();
            for author in publication.authors.iter() {
                if !self
                    .author_map
                    .contains_key(&(author.name.clone(), author.id))
                {
                    continue;
                }
                let auth_id = *self
                    .author_map
                    .get(&(author.name.clone(), author.id))
                    .unwrap();
                if auth_ids_present.contains(&auth_id) {
                    // Filter out duplicate Author
                    continue;
                }
                self.writer
                    .write_publication_author(PublicationAuthorsStore {
                        publication_id: pub_id,
                        author_id: *self
                            .author_map
                            .get(&(author.name.clone(), author.id))
                            .unwrap(),
                    });
                auth_ids_present.push(auth_id);
            }
        }
    }

    /// Read the current person xml tag until finished
    fn read_person(&mut self, eve: &BytesStart) -> Result<(), Box<dyn Error>> {
        let mut buf = Vec::new();
        let mut person = Person::new();
        // Get Modification Date
        person.mdate = String::from(
            eve.try_get_attribute("mdate")
                .unwrap()
                .unwrap()
                .decode_and_unescape_value(self.reader.decoder())?,
        );
        loop {
            // Iterate until tag closes
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    // Add names and aliases
                    b"author" => {
                        let author = self.read_text(&e)?;
                        if person.name == String::new() {
                            person.add_name(author.as_str());
                        } else {
                            person.alias.push(read_person_name(&author));
                        }
                    }
                    b"note" => {
                        // Affiliations
                        let attr = e
                            .try_get_attribute("type")
                            .unwrap()
                            .unwrap_or(Attribute::from(("type", "ERROR")))
                            .decode_and_unescape_value(self.reader.decoder())?;
                        if attr == "affiliation" {
                            let state = String::from(
                                e.try_get_attribute("label")
                                    .unwrap()
                                    .unwrap_or(Attribute::from(("label", "current")))
                                    .decode_and_unescape_value(self.reader.decoder())?,
                            );
                            person.affiliations.push((String::from(attr), state));
                        }
                    }
                    b"url" => {
                        // Homepages
                        let url = self.read_text(&e)?;
                        person.urls.push(url);
                    }
                    _ => {
                        self.reader
                            .read_to_end_into(e.to_end().name(), &mut Vec::new())
                            .unwrap();
                    } // Skip unknown tags
                },
                Ok(Event::End(e)) if e.name().as_ref() == b"www" => break, // End of Tag
                Ok(Event::Eof) => return Err("Unexpected EOF".into()),
                _ => (),
            }
        }
        // Check validity of person
        if person.check_valid() {
            self.write_person(person);
        }
        Ok(())
    }

    /// Write person to csv file
    fn write_person(&mut self, person: Person) {
        self.author_map
            .insert((person.name.clone(), person.id), self.next_author_id);
        // Author
        self.writer.write_author(AuthorStore {
            key: *self
                .author_map
                .get(&(person.name.clone(), person.id))
                .unwrap(),
            name: person.name.clone(),
            id: person.id,
            mdate: person.mdate.clone(),
        });
        // Websites
        for website in person.urls.iter() {
            self.writer.write_author_website(AuthorWebsiteStore {
                id: self.next_author_website_id,
                author_id: *self
                    .author_map
                    .get(&(person.name.clone(), person.id))
                    .unwrap(),
                url: website.clone(),
            });
            self.next_author_website_id += 1;
        }
        // Affiliations
        for affiliation in person.affiliations.iter() {
            self.writer.write_affiliation(AffiliationsStore {
                id: self.next_affiliation_id,
                author_id: *self
                    .author_map
                    .get(&(person.name.clone(), person.id))
                    .unwrap(),
                affiliation: affiliation.0.clone(),
                afftype: affiliation.1.clone(),
            });
            self.next_affiliation_id += 1;
        }
        // Alias
        for alias in person.alias.iter() {
            self.author_map
                .insert((alias.0.clone(), alias.1), self.next_author_id);
            self.writer.write_aliases(AliasStore {
                id: self.next_alias_id,
                author_id: self.next_author_id,
                alias: alias.0.clone(),
                alias_id: alias.1,
            });
            self.next_alias_id += 1;
        }
        self.next_author_id += 1;
    }

    /// Read the text of a tag
    fn read_text(&mut self, start: &BytesStart) -> Result<String, Box<dyn Error>> {
        let mut buf = Vec::new();
        let mut text = String::new();
        loop {
            // Iterate until tag closes
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Text(e)) => {
                    // Read direct Text
                    text += e.unescape()?.into_owned().as_str();
                }
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    // Skip over formatting tags
                    b"i" | b"ref" | b"sup" | b"sub" | b"tt" => {}
                    _ => {
                        return Err(format!(
                            "Unexpected event start {0}",
                            String::from_utf8_lossy(e.name().as_ref())
                        )
                        .into());
                    }
                },
                Ok(Event::End(e)) if e.name().as_ref() == start.name().as_ref() => break, // End loop after end of tag
                Ok(Event::End(e)) => match e.name().as_ref() {
                    // Skip over formatting tags
                    b"i" | b"ref" | b"sup" | b"sub" | b"tt" => {}
                    _ => {
                        return Err(format!(
                            "Unexpected end event {0}",
                            String::from_utf8_lossy(e.name().as_ref())
                        )
                        .into());
                    }
                },
                Ok(Event::Empty(_)) => {} // Skip empty formatting tags
                _ => {
                    return Err(format!(
                        "Unexpected event {0} {1}",
                        self.reader.buffer_position(),
                        self.reader.error_position()
                    )
                    .into());
                }
            }
        }
        Ok(text)
    }

    /// Read the text of a tag as Integer
    fn read_int(&mut self, start: &BytesStart) -> Result<usize, Box<dyn Error>> {
        let value = self.read_text(start)?;
        Ok(value.parse::<usize>().unwrap_or_else(|e1| {
            let name = String::from_utf8_lossy(start.name().as_ref()).into_owned();
            panic!("key: {name} value:{value} {e1}")
        }))
    }
}

/// Datastruct for all Publication relevant data
#[derive(Serialize, Deserialize)]
pub struct Publication {
    pubtype: String, // Bibtex type
    key: String,
    mdate: String, // Modification Date
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
    references: Vec<(String, usize)>, // cite, crossref
    resources: Vec<(String, String)>, // ee, url, note (without isbn tagged notes), series, stream
    authors: Vec<Person>,
}

impl Publication {
    /// Create a new Publication
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

    /// Check if has at least a [`Publication::pubtype`], [`Publication::key`], [`Publication::mdate`] and [`Publication::title`].
    pub fn fulfills_constraints(&self) -> bool {
        !self.pubtype.is_empty()
            && !self.key.is_empty()
            && !self.mdate.is_empty()
            && !self.title.is_empty()
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

/// Datastruct for all relevant Person data
#[derive(Serialize, Deserialize, PartialEq)]
pub struct Person {
    name: String,
    id: usize,
    alias: Vec<(String, usize)>,
    mdate: String,
    affiliations: Vec<(String, String)>,
    urls: Vec<String>,
}

impl Person {
    /// Create a new Person
    fn new() -> Person {
        Person {
            name: String::new(),
            id: 0,
            alias: Vec::new(),
            mdate: String::new(),
            affiliations: Vec::new(),
            urls: Vec::new(),
        }
    }

    /// Add Name to Person
    fn add_name(&mut self, name: &str) {
        (self.name, self.id) = read_person_name(name);
    }

    /// Check if has at least [`Person::name`]
    fn check_valid(&self) -> bool {
        if self.name.is_empty() {
            return false;
        }
        true
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

/// Manages the writing of relevant data into csv files
struct WriteManager {
    venues: Vec<VenueStore>,
    publishers: Vec<PublisherStore>,
    editors: Vec<EditorStore>,
    authors: Vec<AuthorStore>,
    publications: Vec<PublicationStore>,
    resources: Vec<ResourceStore>,
    publication_editors: Vec<PublicationEditorStore>,
    references: Vec<ReferenceStore>,
    publication_authors: Vec<PublicationAuthorsStore>,
    author_websites: Vec<AuthorWebsiteStore>,
    affiliations: Vec<AffiliationsStore>,
    aliases: Vec<AliasStore>,
}

impl WriteManager {
    /// Create new WriterManager
    pub fn new() -> WriteManager {
        // Initialize WriterManager with empty vectors
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

    /// Writes venue first to WriterManager and then to tsv file
    pub fn write_venue(&mut self, venue: VenueStore) {
        // Add venue to list
        self.venues.push(venue);
        // If list of venue exceeds 10000 write all of them to file
        if self.venues.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new().append(true).create(true).open(VENUE_FILE).unwrap());
            for v in &self.venues {
                wrt.serialize(v).unwrap();
            }
            wrt.flush().unwrap();
            self.venues.clear()
        }
    }

    /// Writes publisher first to WriterManager and then to tsv file
    pub fn write_publisher(&mut self, publisher: PublisherStore) {
        // Add publisher to list
        self.publishers.push(publisher);
        // If list of publishers exceeds 10000 write all of them to file
        if self.publishers.len() == 10000 {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(PUBLISHER_FILE)
                    .unwrap(),
            );
            for p in &self.publishers {
                wrt.serialize(p).unwrap();
            }
            wrt.flush().unwrap();
            self.publishers.clear()
        }
    }

    /// Writes editor first to WriterManager and then to tsv file
    pub fn write_editor(&mut self, editor: EditorStore) {
        // Add editors to list
        self.editors.push(editor);
        // If list of editors exceeds 10000 write all of them to file
        if self.editors.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new().append(true).create(true).open(EDITOR_FILE).unwrap());
            for e in &self.editors {
                wrt.serialize(e).unwrap();
            }
            wrt.flush().unwrap();
            self.editors.clear()
        }
    }

    /// Writes author first to WriterManager and then to tsv file
    pub fn write_author(&mut self, author: AuthorStore) {
        // Add author to list
        self.authors.push(author);
        // If list of authors exceeds 10000 write all of them to file
        if self.authors.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new().append(true).create(true).open(AUTHOR_FILE).unwrap());
            for a in &self.authors {
                wrt.serialize(a).unwrap();
            }
            wrt.flush().unwrap();
            self.authors.clear()
        }
    }

    /// Writes publication first to WriterManager and then to tsv file
    pub fn write_publication(&mut self, publication: PublicationStore) {
        // Add publication to list
        self.publications.push(publication);
        // If list of authors exceeds 10000 write all of them to file
        if self.publications.len() == 10000 {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(PUBLICATION_FILE)
                    .unwrap(),
            );
            for p in &self.publications {
                wrt.serialize(p).unwrap();
            }
            wrt.flush().unwrap();
            self.publications.clear()
        }
    }

    /// Writes resource first to WriterManager and then to tsv file
    pub fn write_resource(&mut self, resource: ResourceStore) {
        // Add resource to list
        self.resources.push(resource);
        // If list of resources exceeds 10000 write all of them to file
        if self.resources.len() == 10000 {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(RESOURCES_FILE)
                    .unwrap(),
            );
            for r in &self.resources {
                wrt.serialize(r).unwrap();
            }
            wrt.flush().unwrap();
            self.resources.clear()
        }
    }

    /// Writes publication editor first to WriterManager and then to tsv file
    pub fn write_publication_editor(&mut self, pub_editor: PublicationEditorStore) {
        // Add publication editor to list
        self.publication_editors.push(pub_editor);
        // If list of publication editors exceeds 10000 write all of them to file
        if self.publication_editors.len() == 10000 {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(PUBLICATION_EDITOR_FILE)
                    .unwrap(),
            );
            for p in &self.publication_editors {
                wrt.serialize(p).unwrap();
            }
            wrt.flush().unwrap();
            self.publication_editors.clear()
        }
    }

    /// Writes reference first to WriterManager and then to tsv file
    pub fn write_reference(&mut self, reference: ReferenceStore) {
        // Add refrence to list
        self.references.push(reference);
        // If list of refrences exceeds 10000 write all of them to file
        if self.references.len() == 10000 {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(REFERENCE_FILE)
                    .unwrap(),
            );
            for r in &self.references {
                wrt.serialize(r).unwrap();
            }
            wrt.flush().unwrap();
            self.references.clear()
        }
    }

    /// Writes publication author first to WriterManager and then to tsv file
    pub fn write_publication_author(&mut self, pub_auth: PublicationAuthorsStore) {
        // Add publication author to list
        self.publication_authors.push(pub_auth);
        // If list of publication authors exceeds 10000 write all of them to file
        if self.publication_authors.len() == 10000 {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(PUBLICATION_AUTHORS_FILE)
                    .unwrap(),
            );
            for p in &self.publication_authors {
                wrt.serialize(p).unwrap();
            }
            wrt.flush().unwrap();
            self.publication_authors.clear()
        }
    }

    /// Writes author website first to WriterManager and then to tsv file
    pub fn write_author_website(&mut self, auth_web: AuthorWebsiteStore) {
        // Add author website to list
        self.author_websites.push(auth_web);
        // If list of author websites exceeds 10000 write all of them to file
        if self.author_websites.len() == 10000 {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(AUTHOR_WEBSITES_FILE)
                    .unwrap(),
            );
            for aw in &self.author_websites {
                wrt.serialize(aw).unwrap();
            }
            wrt.flush().unwrap();
            self.author_websites.clear()
        }
    }

    /// Writes affiliation first to WriterManager and then to tsv file
    pub fn write_affiliation(&mut self, aff: AffiliationsStore) {
        // Add affiliation to list
        self.affiliations.push(aff);
        // If list of affiliations exceeds 10000 write all of them to file
        if self.affiliations.len() == 10000 {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(AFFILIATIONS_FILE)
                    .unwrap(),
            );
            for a in &self.affiliations {
                wrt.serialize(a).unwrap();
            }
            wrt.flush().unwrap();
            self.affiliations.clear()
        }
    }

    /// Writes aliases first to WriterManager and then to tsv file
    pub fn write_aliases(&mut self, alias: AliasStore) {
        // Add aliases to list
        self.aliases.push(alias);
        // If list of aliases exceeds 10000 write them to file
        if self.aliases.len() == 10000 {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new().append(true).create(true).open(ALIAS_FILE).unwrap());
            for a in &self.aliases {
                wrt.serialize(a).unwrap();
            }
            wrt.flush().unwrap();
            self.aliases.clear()
        }
    }

    /// Write the remaining items contained in any list to corresponding file
    pub fn finalize(&mut self) {
        // Venues
        if !self.venues.is_empty() {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new().append(true).create(true).open(VENUE_FILE).unwrap());
            for venue in &self.venues {
                wrt.serialize(venue).unwrap();
            }
            wrt.flush().unwrap();
            self.venues.clear()
        }
        // Publishers
        if !self.publishers.is_empty() {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(PUBLISHER_FILE)
                    .unwrap(),
            );
            for tuple in &self.publishers {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publishers.clear()
        }
        // Editors
        if !self.editors.is_empty() {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new().append(true).create(true).open(EDITOR_FILE).unwrap());
            for tuple in &self.editors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.editors.clear()
        }
        // Affiliations
        if !self.affiliations.is_empty() {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(AFFILIATIONS_FILE)
                    .unwrap(),
            );
            for tuple in &self.affiliations {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.affiliations.clear()
        }
        // Authors
        if !self.authors.is_empty() {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new().append(true).create(true).open(AUTHOR_FILE).unwrap());
            for tuple in &self.authors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.authors.clear()
        }
        // Publications
        if !self.publications.is_empty() {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(PUBLICATION_FILE)
                    .unwrap(),
            );
            for tuple in &self.publications {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publications.clear()
        }
        // Resources
        if !self.resources.is_empty() {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(RESOURCES_FILE)
                    .unwrap(),
            );
            for tuple in &self.resources {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.resources.clear()
        }
        // Publication Editors
        if !self.publication_editors.is_empty() {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(PUBLICATION_EDITOR_FILE)
                    .unwrap(),
            );
            for tuple in &self.publication_editors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publication_editors.clear()
        }
        // References
        if !self.references.is_empty() {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(REFERENCE_FILE)
                    .unwrap(),
            );
            for tuple in &self.references {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.references.clear()
        }
        // Publication Authors
        if !self.publication_authors.is_empty() {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(PUBLICATION_AUTHORS_FILE)
                    .unwrap(),
            );
            for tuple in &self.publication_authors {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.publication_authors.clear()
        }
        // Author Websites
        if !self.author_websites.is_empty() {
            let mut wrt = WriterBuilder::new().delimiter(b'\t').from_writer(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(AUTHOR_WEBSITES_FILE)
                    .unwrap(),
            );
            for tuple in &self.author_websites {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.author_websites.clear()
        }
        // Aliases
        if !self.aliases.is_empty() {
            let mut wrt = WriterBuilder::new()
                .delimiter(b'\t')
                .from_writer(OpenOptions::new().append(true).create(true).open(ALIAS_FILE).unwrap());
            for tuple in &self.aliases {
                wrt.serialize(tuple).unwrap();
            }
            wrt.flush().unwrap();
            self.aliases.clear()
        }
    }
}

#[derive(Serialize)]
struct VenueStore {
    id: usize,
    name: Option<String>,
    #[serde(rename = "type")]
    venue_type: Option<String>,
}

#[derive(Serialize)]
struct PublisherStore {
    id: usize,
    name: Option<String>,
}

#[derive(Serialize)]
struct EditorStore {
    id: usize,
    name: String,
}

#[derive(Serialize)]
struct AuthorStore {
    key: usize,
    id: usize,
    name: String,
    mdate: String,
}

#[derive(Serialize)]
struct PublicationStore {
    id: usize,
    key: String,
    mdate: String,
    title: String,
    year: Option<usize>,
    month: Option<String>,
    #[serde(rename = "type")]
    pub_type: String,
    school: Option<String>,
    isbn: Option<String>,
    pages: Option<String>,
    volume: Option<String>,
    number: Option<String>,
    venue_id: Option<usize>,
    publisher_id: Option<usize>,
}

#[derive(Serialize)]
struct ResourceStore {
    id: usize,
    #[serde(rename = "type")]
    res_type: String,
    value: String,
    publication_id: usize,
}

#[derive(Serialize)]
struct PublicationEditorStore {
    publication_id: usize,
    editor_id: usize,
}

#[derive(Serialize)]
struct ReferenceStore {
    #[serde(rename = "type")]
    ref_type: String,
    origin_pub: usize,
    dest_pub: usize,
}

#[derive(Serialize)]
struct PublicationAuthorsStore {
    publication_id: usize,
    author_id: usize,
}

#[derive(Serialize)]
struct AuthorWebsiteStore {
    id: usize,
    author_id: usize,
    url: String,
}

#[derive(Serialize)]
struct AffiliationsStore {
    id: usize,
    author_id: usize,
    affiliation: String,
    #[serde(rename = "type")]
    afftype: String,
}

#[derive(Serialize)]
struct AliasStore {
    id: usize,
    author_id: usize,
    alias: String,
    alias_id: usize,
}

/// Read the persons name and generate the id for him if needed.
/// DBLP writes duplicate names as: "Name 0001", "Name 0002" etc.
fn read_person_name(name: &str) -> (String, usize) {
    let re = Regex::new(r"(.*)\s+(\d+)").unwrap();
    let name = name.trim();
    // There exists an ID
    if let Some(caps) = re.captures(name) {
        let name = caps.get(1).unwrap().as_str().to_string();
        let id = caps.get(2).unwrap().as_str().parse::<usize>().unwrap();
        (name, id)
    } else {
        // No ID specified
        let name = name.to_string();
        (name, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_eq;

    #[test]
    fn test_read_person_no_id() {
        let (name_r, id_r) = read_person_name("Max Mustermann");
        assert_eq!(name_r, "Max Mustermann");
        assert_eq!(id_r, 0);
    }

    #[test]
    fn test_read_person_with_id() {
        let (name_r, id_r) = read_person_name("Max Mustermann 0001");
        assert_eq!(name_r, "Max Mustermann");
        assert_eq!(id_r, 1);
    }
}
