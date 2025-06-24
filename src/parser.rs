use std::array::IntoIter;
use quick_xml::Reader;
use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesStart, Event};
use regex::Regex;
use std::error::Error;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;

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
    pub fn generate_sql_ops(
        &self,
        param_char: char,
    ) -> (Vec<(String, Vec<String>)>, Vec<(String, Vec<String>)>) {
        match self {
            Record::Publication(publication) => publication.to_owned().generate_sql_ops(param_char),
            Record::Person(person) => (person.to_owned().generate_sql_ops(param_char), Vec::new()),
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

    pub fn generate_sql_ops(
        &self,
        param_char: char,
    ) -> (Vec<(String, Vec<String>)>, Vec<(String, Vec<String>)>) {
        let mut sql_ops = Vec::new();
        let mut ref_sql_ops = Vec::new();
        // Venues
        let mut venue_name = String::new();
        let mut venue_type = String::new();
        match self.pubtype.as_ref() {
            "article" => {
                venue_name = self.journal.clone();
                venue_type = "journal".to_string();
            }
            "inproceedings" | "proceedings" => {
                venue_name = self.book_title.clone();
                venue_type = "conference".to_string();
            }
            "incollection" => {
                venue_name = self.book_title.clone();
                venue_type = "book".to_string();
            }
            _ => (),
        };
        // Venue
        if !venue_name.is_empty() && !venue_type.is_empty() {
            sql_ops.push((
                format!(
                    "INSERT INTO Venues (name, type) VALUES({0}1, {0}2) ON CONFLICT DO NOTHING;",
                    param_char
                ),
                vec![venue_name.clone(), venue_type.clone()],
            ));
        }
        // Publisher
        if !self.publisher.is_empty() {
            sql_ops.push((
                format!(
                    "INSERT INTO Publishers (name) VALUES ({0}1) ON CONFLICT DO NOTHING;",
                    param_char
                ),
                vec![self.publisher.clone()],
            ));
        }
        // Publication
        let mut extra_keys = String::new();
        let mut extra_values = String::new();
        let mut extra_params = Vec::new();
        let mut count = 5;
        if self.year != 0 {
            extra_keys.push_str(", year");
            extra_params.push(self.year.to_string());
            extra_values.push_str(format!(", {0}{1}", param_char, count).as_str());
            count += 1;
        }
        if !self.month.is_empty() {
            extra_keys.push_str(", month");
            extra_params.push(self.month.clone());
            extra_values.push_str(format!(", {0}{1}", param_char, count).as_str());
            count += 1;
        }
        if !self.school.is_empty() {
            extra_keys.push_str(", school");
            extra_params.push(self.school.clone());
            extra_values.push_str(format!(", {0}{1}", param_char, count).as_str());
            count += 1;
        }
        if !self.isbn.is_empty() {
            extra_keys.push_str(", isbn");
            extra_params.push(self.isbn.clone());
            extra_values.push_str(format!(", {0}{1}", param_char, count).as_str());
            count += 1;
        }
        if !self.pages.is_empty() {
            extra_keys.push_str(", pages");
            extra_params.push(self.pages.clone());
            extra_values.push_str(format!(", {0}{1}", param_char, count).as_str());
            count += 1;
        }
        if !self.volume.is_empty() {
            extra_keys.push_str(", volume");
            extra_params.push(self.volume.to_string());
            extra_values.push_str(format!(", {0}{1}", param_char, count).as_str());
            count += 1;
        }
        if !self.number.is_empty() {
            extra_keys.push_str(", number");
            extra_params.push(self.number.to_string());
            extra_values.push_str(format!(", {0}{1}", param_char, count).as_str());
            count += 1;
        }
        if !venue_name.is_empty() && !venue_type.is_empty() {
            extra_keys.push_str(", venue_id");
            extra_values.push_str(
                format!(
                    ", (SELECT id FROM Venues WHERE name={0}{1} AND type={0}{2})",
                    param_char,
                    count,
                    count + 1
                )
                .as_str(),
            );
            extra_params.push(venue_name);
            extra_params.push(venue_type);
            count += 2;
        }
        if !self.publisher.is_empty() {
            extra_keys.push_str(", publisher_id");
            extra_values.push_str(
                format!(
                    ", (SELECT id FROM Publishers WHERE name= {0}{1})",
                    param_char, count
                )
                .as_str(),
            );
            extra_params.push(self.publisher.clone());
        }

        let mut params = vec![
            self.key.clone(),
            self.mdate.clone(),
            self.title.clone(),
            self.pubtype.clone(),
        ];
        params.extend(extra_params);
        sql_ops.push(
            (format!(
                "INSERT INTO Publications (key, mdate, title, type{1}) VALUES ({0}1, {0}2, {0}3, {0}4{2}) ON CONFLICT DO NOTHING;",
                param_char,
                extra_keys,
                extra_values
            ),
                params
            )
        );
        // Authors
        for author in &self.authors {
            sql_ops.push((
                format!(
                    "INSERT INTO Authors (name, id) VALUES ({0}1, {0}2) ON CONFLICT DO NOTHING;",
                    param_char
                ),
                vec![author.name.clone(), author.id.clone()],
            ));
            ref_sql_ops.push((
                format!(
                    "INSERT INTO PublicationAuthors (publication_key, author_id) VALUES ({0}1,\
                      (SELECT id FROM Authors WHERE name = {0}2 AND id = {0}3)\
                      ) ON CONFLICT DO NOTHING;",
                    param_char
                ),
                vec![self.key.clone(), author.name.clone(), author.id.clone()],
            ));
        }
        // Resources
        for resource in &self.resources {
            sql_ops.push((
                format!(
                    "INSERT INTO Resources (type, value, publication_key) VALUES ({0}1, {0}2, {0}3) ON CONFLICT DO NOTHING;", param_char),
                    vec![resource.0.clone(), resource.1.clone(), self.key.clone()]
                )
            );
        }
        // Refrences
        for reference in &self.references {
            ref_sql_ops.push((
                format!(
                    "INSERT INTO Reference (type, origin_pub, dest_pub) VALUES ({0}1, {0}2, {0}3) ON CONFLICT DO NOTHING;", param_char),
                    vec![reference.0.clone(), self.key.clone(), reference.1.clone()]
                )
            );
        }
        // Editors
        for editor in &self.editor {
            sql_ops.push((
                format!(
                    "INSERT INTO Editors (name) VALUES ({0}1) ON CONFLICT DO NOTHING;",
                    param_char
                ),
                vec![editor.clone()],
            ));
            ref_sql_ops.push((
                format!(
                    "INSERT INTO PublicationEditors (publication_key, editor_id) VALUES ({0}1, \
                    (SELECT id FROM Editors WHERE name = {0}2)\
                    ) ON CONFLICT DO NOTHING;",
                    param_char
                ),
                vec![self.key.clone(), editor.clone()],
            ));
        }
        (sql_ops, ref_sql_ops)
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

    fn generate_sql_ops(&self, param_char: char) -> Vec<(String, Vec<String>)> {
        let mut sql_ops = Vec::new();

        // Author
        sql_ops.push((
            format!(
                "INSERT INTO Authors(name, id, mdate) VALUES ({0}1 , {0}2 ,{0}3) ON CONFLICT DO UPDATE \
                SET mdate = excluded.mdate;",
                param_char
            ),
            vec![self.name.clone(), self.id.clone(), self.mdate.clone()]
        )
        );

        // Affiliations
        if !self.affiliations.is_empty() {
            for affiliation in &self.affiliations {
                sql_ops.push((
                    format!(
                        "INSERT INTO Affiliations( author_id, affiliation, type)\
                             VALUES ( \
                             (SELECT key FROM Authors WHERE name= {0}1 AND id= {0}2 ),\
                             {0}3, {0}4);",
                        param_char,
                    ),
                    vec![
                        self.name.clone(),
                        self.id.clone(),
                        affiliation.0.clone(),
                        affiliation.1.clone(),
                    ],
                ))
            }
        }

        // AuthorWebsites
        if !self.urls.is_empty() {
            for url in &self.urls {
                sql_ops.push((
                    format!(
                        "INSERT INTO AuthorWebsites (author_id, url) VALUES ( \
                    (SELECT key FROM Authors WHERE name= {0}1 AND id= {0}2),\
                    {0}3);",
                        param_char,
                    ),
                    vec![self.name.clone(), self.id.clone(), url.clone()],
                ))
            }
        }

        // Alias
        if !self.alias.is_empty() {
            for alias in &self.alias {
                sql_ops.push((
                    format!(
                        "INSERT INTO Alias (author_id, alias) VALUES ( \
                    (SELECT key FROM Authors WHERE name= {0}1 AND id={0}2),\
                    {0}3);",
                        param_char
                    ),
                    vec![self.name.clone(), self.id.clone(), alias.clone()],
                ))
            }
        }

        sql_ops
    }
}