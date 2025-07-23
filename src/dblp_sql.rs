use std::collections::{HashMap, HashSet, VecDeque};
use postgres_types::{FromSql, ToSql};
use crate::parser::{Parser, Record};
#[cfg(feature="duckdb")]
use duckdb::{
    ToSql as DuckToSql,
    types::ToSqlOutput
};


pub struct DataManager {
    visited_keys: HashSet<Key>,
    items: Vec<DataItem>,
    parser: Parser,
    queue:  VecDeque<Vec<DataItem>>,
    chunk_size: usize,
    total_items: usize,
    processed_items: usize,
}

impl DataManager {
    pub fn new( parser: Parser) -> Self {
        DataManager {
            visited_keys: HashSet::new(),
            items: Vec::new(),
            parser,
            queue:  VecDeque::new(),
            chunk_size: 50000,
            total_items: 0,
            processed_items: 0,
        }
    }
    
    fn move_satisified_to_queue(&mut self){
        let condition = |x: &DataItem| x.depends_on.is_subset(&self.visited_keys);
        let mut satisified = Vec::new();
        let mut i = 0;
        while i < self.items.len() {
            if condition(&self.items[i]) {
                satisified.push(self.items.swap_remove(i));
            }
            else { 
                i += 1;
            }
        }
        for group in self.group_by_type(satisified) {
            self.queue.push_back(group);
        }
    }
    
    fn group_by_type(&mut self, data: Vec<DataItem>) -> Vec<Vec<DataItem>> {
        let mut grouping: HashMap<String, Vec<DataItem>> = HashMap::new();
        for item in data {
            if grouping.contains_key(&item.value.matcher()) {
                grouping.get_mut(&item.value.matcher()).unwrap().push(item);
            } else {
                grouping.insert(item.value.matcher(), vec![item]);
            }
        }
        grouping.into_iter().map(|(_, group)| group).collect()
    }
    
    fn insert_record(&mut self, record: Record) {
        let mut data_items = record.generate_data_items();
        self.total_items += data_items.len();
        self.items.append(&mut data_items)
    }
    
    pub fn set_chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = chunk_size;
    }
    
    pub fn log(&self) {
        println!("processed items: {} / total_items: {}", self.processed_items, self.total_items);
    }
}

impl Iterator for DataManager {
    type Item = Vec<DataItem>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.queue.is_empty() {
            for _ in 0..self.chunk_size {
                if let Some(record) = self.parser.next() {
                    self.insert_record(record);
                } 
                else { 
                    break;
                }
            }
            self.move_satisified_to_queue();
        }
        
        if let Some(data_item) = self.queue.pop_front() {
            self.processed_items += data_item.len();
            Some(data_item)
        } 
        else {
            None
        }
    }
}

pub struct DataItem {
    pub depends_on: HashSet<Key>,
    pub value: Data
}

impl DataItem {
    pub fn new(value: Data) -> Self {
        DataItem { depends_on: HashSet::new(), value }
    }
    
    pub fn add_depends_on(&mut self, depends_on: Key) {
        self.depends_on.insert(depends_on);
    }
}

pub enum Data {
    Venue(Venue),
    Publication(Publication),
    Publisher(Publisher),
    Editor(Editor),
    Author(Author),
    Reference(Reference),
    Resource(Resource),
    PublicationEditor(PublicationEditor),
    PublicationAuthor(PublicationAuthor),
    AuthorWebsite(AuthorWebsite),
    Affiliation(Affiliation),
    Alias(Alias),
}

impl Data {
    pub fn matcher(&self) -> String {
        match self {
            Data::Venue(_) => { "venue".to_string() }
            Data::Publication(_) => { "publication".to_string() }
            Data::Publisher(_) => {"publisher".to_string() }
            Data::Editor(_) => { "editor".to_string() }
            Data::Author(_) => { "author".to_string() }
            Data::Reference(_) => { "reference".to_string() }
            Data::Resource(_) => {"resource".to_string() }
            Data::PublicationEditor(_) => {"publicationEditor".to_string() }
            Data::PublicationAuthor(_) => {"publicationAuthor".to_string() }
            Data::AuthorWebsite(_) => {"authorWebsite".to_string() }
            Data::Affiliation(_) => {"affiliation".to_string() }
            Data::Alias(_) => {"alias".to_string() }
        }
    }
    
    pub fn key(&self) -> Key {
        match self {
            Data::Venue(x) => x.key(),
            Data::Publisher(x) => x.key(),
            Data::Author(x) => x.key(),
            Data::Publication(x) => x.key(),
            Data::Editor(x) => x.key(),
            _ => panic!("Not Implemented key function"),
        }
    }
}

#[derive(Debug, ToSql, FromSql, Eq, Hash, PartialEq, Clone)]
#[postgres(name = "pub_type", rename_all = "lowercase")]
pub enum PublicationType {
    Article,
    InProceedings,
    Proceedings,
    Book,
    InCollection,
    PHDThesis,
    MasterThesis,
    WWW,
}

impl PublicationType {
    pub fn from_str(s: &str) -> Option<PublicationType> {
        match s {
            "article" => Some(PublicationType::Article),
            "inproceedings" => Some(PublicationType::InProceedings),
            "proceedings" => Some(PublicationType::Proceedings),
            "book" => Some(PublicationType::Book),
            "incollection" => Some(PublicationType::InCollection),
            "phdthesis" => Some(PublicationType::PHDThesis),
            "masterthesis" => Some(PublicationType::MasterThesis),
            "www" => Some(PublicationType::WWW),
            _ => None,
        }
    }
}
#[cfg(feature="duckdb")]

impl DuckToSql for PublicationType {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        let status_str = match self {
            PublicationType::Article => "article",
            PublicationType::InProceedings => "inproceedings",
            PublicationType::Proceedings => "proceedings",
            PublicationType::Book => "book",
            PublicationType::InCollection => "incollection",
            PublicationType::PHDThesis => "phdthesis",
            PublicationType::MasterThesis => "masterthesis",
            PublicationType::WWW => "www",
        };
        
        duckdb::Result::Ok(ToSqlOutput::from(status_str))
    }
}

#[derive(Debug, ToSql, FromSql, Eq, Hash, PartialEq, Clone, Copy)]
#[postgres(name = "venue_type", rename_all = "lowercase")]
pub enum VenueType {
    Journal,
    Conference,
    Book,
}

impl VenueType {
    pub fn to_str(&self) -> String {
        match self {
            VenueType::Journal => "journal".to_string(),
            VenueType::Conference => "conference".to_string(),
            VenueType::Book => "book".to_string(),
        }
    }
}

#[cfg(feature="duckdb")]
impl DuckToSql for VenueType {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        let status_str = match self {
            VenueType::Journal => "journal",
            VenueType::Conference => "conference",
            VenueType::Book => "book",
        };
        duckdb::Result::Ok(ToSqlOutput::from(status_str))
    }
}

#[derive(Debug, ToSql, FromSql, Eq, Hash, PartialEq, Clone)]
#[postgres(name = "ref_type")]
pub enum RefrenceType {
    #[postgres(name = "crossref")]
    CrossReference,
    #[postgres(name = "cite")]
    Citation,
}

impl RefrenceType {
    pub fn from_str(s: &str) -> Option<RefrenceType> {
        match s {
            "crossref" => Some(RefrenceType::CrossReference),
            "cite" => Some(RefrenceType::Citation),
            _ => None,
        }
    }
}

#[cfg(feature="duckdb")]
impl DuckToSql for RefrenceType {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        let status_str = match self {
            RefrenceType::CrossReference => "crossref",
            RefrenceType::Citation => "cite",
        };
        
        duckdb::Result::Ok(ToSqlOutput::from(status_str))
    }
}

#[derive(Debug, ToSql, FromSql, Eq, Hash, PartialEq, Clone)]
#[postgres(name = "aff_type", rename_all = "lowercase")]
pub enum AffiliationType {
    Current,
    Former,
}

impl AffiliationType {
    pub fn from_str(s: &str) -> Option<AffiliationType> {
        match s {
            "current" => Some(AffiliationType::Current),
            "former" => Some(AffiliationType::Former),
            _ => None,
        }
    }
}

#[cfg(feature="duckdb")]
impl DuckToSql for AffiliationType {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        let status_str = match self {
            AffiliationType::Current => "current",
            AffiliationType::Former => "former",
        };
        
        duckdb::Result::Ok(ToSqlOutput::from(status_str))
    }
}

pub struct Venue {
    pub name: String,
    pub venue_type: VenueType
}

impl Venue {
    pub fn key(&self) -> Key {
        Key::Venue(
            VenueKey {
                key: self.name.clone(),
                venue_type: self.venue_type.clone(),
            }
        )
    }
}

pub struct Publisher {
    pub name: String,
}

impl Publisher {
    pub fn key(&self) -> Key {
        Key::Publisher(
            PublisherKey {
                key: self.name.clone(),
            })
    }
}

pub struct Editor {
    pub name: String,
}

impl Editor {
    pub fn key(&self) -> Key {
        Key::Editor(
            EditorKey {
                key: self.name.clone(),
            })
    }
}

pub struct Author {
    pub name: String,
    pub id: String,
    pub mdate: String,
}

impl Author {
    pub fn key(&self) -> Key {
        Key::Author(
            AuthorKey {
                name: self.name.clone(),
                id: self.id.clone(),
            })
    }
}

pub struct Publication {
    pub key: String,
    pub mdate: String,
    pub title: String,
    pub pub_type: PublicationType,
    pub year: Option<u32>,
    pub month: Option<String>,
    pub school: Option<String>,
    pub isbn: Option<String>,
    pub pages: Option<String>,
    pub volume: Option<String>,
    pub number: Option<String>,
    pub venue: Option<VenueKey>,
    pub publisher: Option<PublisherKey>,
}

impl Publication {
    pub fn key(&self) -> Key {
        Key::Publication(
            PublicationKey {
                key: self.key.clone(),
            }
        )
    }
}

impl Publication {
    pub fn get_venue_key(&self) -> Option<VenueKey> {
        if  let Some(v) = &self.venue {
            Some(v.clone())
        } else { 
            None
        }
    }
    
    pub fn get_publisher_key(&self) -> Option<PublisherKey> {
        if  let Some(v) = &self.publisher {
            Some(v.clone())
        } else { 
            None
        }
    }
}

pub struct Resource {
    pub resource_type: String,
    pub value: String,
    pub publication: PublicationKey,
}

pub struct PublicationEditor {
    pub publication: PublicationKey,
    pub editor: EditorKey,
}

pub struct Reference {
    pub refrence_type: RefrenceType,
    pub origin: PublicationKey,
    pub destination: PublicationKey,
}

pub struct PublicationAuthor {
    pub publication: PublicationKey,
    pub author: AuthorKey,
}

pub struct AuthorWebsite {
    pub url: String,
    pub author: AuthorKey,
}

pub struct Affiliation {
    pub author: AuthorKey,
    pub affiliation: String,
    pub aff_type: AffiliationType,
}

pub struct Alias {
    pub author: AuthorKey,
    pub alias: String,
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub enum Key {
    Author(AuthorKey),
    Publication(PublicationKey),
    Editor(EditorKey),
    Publisher(PublisherKey),
    Venue(VenueKey),
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct AuthorKey {
    pub name: String,
    pub id: String,
}

impl AuthorKey {
    pub fn to_string(&self) -> String {
        format!("{}{}", self.name, self.id)
    }
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct PublicationKey {
    pub key: String,
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct EditorKey {
    pub key: String,
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct PublisherKey {
    pub key: String,
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct VenueKey {
    pub key: String,
    pub venue_type: VenueType,
}

impl VenueKey {
    pub fn get_string(&self) -> String {
        let string = self.key.clone();
        string + self.venue_type.to_str().as_str()
    }
}