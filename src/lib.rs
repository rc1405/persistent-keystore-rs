use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::SeekFrom;
use std::time::{SystemTime, Duration};
use std::collections::HashMap;
use std::io::prelude::*;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::fs::OpenOptions;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use tracing::{debug, error, info, trace};

mod structs;
pub mod errors;
pub mod mocks;
pub use structs::*;
use errors::*;
use std::thread::JoinHandle;

struct Saver {
    handle: Option<JoinHandle<()>>,
    killer: std::sync::mpsc::SyncSender<()>,
}

impl Drop for Saver {
    fn drop(&mut self) {
        self.killer.send(()).unwrap();
        if let Some(h) = self.handle.take() {
            h.join().unwrap();
        }
    }
}

/// Thread-safe, optionally persistent client for interacting with a keystore database
#[derive(Clone)]
pub struct Client {
    database: Arc<Mutex<Database>>,
    raw_file: Arc<Mutex<PathBuf>>,
    handle: Arc<Option<Saver>>,
}

fn open_file<P: AsRef<Path> + Clone + std::fmt::Debug>(path: P) -> Result<File, std::io::Error> {
    debug!("Opening file {:?}", path);
    OpenOptions::new()
        .write(true)
        .read(true)
        .create(false)
        .truncate(false)
        .append(false)
        .open(path)
}

impl Client {
    /// Creates a database at the supplied path
    /// ```
    /// # use persistent_keystore_rs::Client;
    /// use std::path::Path;
    /// let c = Client::new(Path::new("temp.db"), None);
    /// # std::fs::remove_file("temp.db").unwrap();
    /// ```
    /// This database will not sync on its own and will need to be saved with 
    /// ```
    /// # use persistent_keystore_rs::Client;
    /// # let mut c = Client::new(std::path::Path::new("temp2.db"), None).unwrap();
    /// c.save();
    /// # std::fs::remove_file("temp2.db").unwrap();
    /// ```
    /// 
    /// To create a persistent database that will create a thread attached to the lifetime
    /// of the client provide a duration
    /// ```
    /// # use persistent_keystore_rs::Client;
    /// use std::path::Path;
    /// use std::time::Duration;
    /// let c = Client::new(Path::new("temp3.db"), Some(Duration::from_millis(30)));
    /// # drop(c);
    /// # std::fs::remove_file("temp3.db").unwrap();
    /// ```
    /// This thread will prune (remove stale entries) and save the database
    /// every duration
    pub fn new<P: AsRef<Path> + Clone + std::fmt::Debug>(path: P, sync_interval: Option<Duration>) -> Result<Self, DatabaseError> {
        info!("Creating Client with database at {:?}", path);
        if path.as_ref().exists() {
            error!("Database exists, cannot create: {:?}", path);
            return Err(DatabaseError::DatabaseExistsError)
        };

        let mut database = Database::default();
        
        if let Some(d) = sync_interval {
            debug!("Setting sync interval to {:?}", d);
            database.set_sync_duration(d);
        };

        let mut client = Self{
            database: Arc::new(Mutex::new(database)),
            raw_file: Arc::new(Mutex::new(PathBuf::from(path.as_ref()))),
            handle: Arc::new(None),
        };

        if let Some(d) = sync_interval {
            let mut c = client.clone();
            let (tx, rx) = std::sync::mpsc::sync_channel(0);
            let h = std::thread::spawn( move || loop {
                    if let Ok(_) = rx.try_recv() {
                        trace!("Breaking");
                        break
                    };

                    trace!("Sleeping for {:?}", d);
                    sleep(d);

                    trace!("Pruning database");
                    c.prune().unwrap();
                    debug!("Database pruned");
                    
                    trace!("Saving database");
                    c.save().unwrap();
                    debug!("Database saved");
                }
            );
            client.handle = Arc::new(Some(Saver{
                handle: Some(h),
                killer: tx,
            }));
        };

        client.save()?;
        trace!("Returning Client");
        Ok(client)
    }

    /// Opens an existing database at the supplied path
    /// ```
    /// # use persistent_keystore_rs::Client;
    /// use std::path::Path;
    /// # let c = Client::new(Path::new("existing.db"), None);
    /// # drop(c);
    /// let c = Client::open(Path::new("existing.db"));
    /// # std::fs::remove_file("existing.db").unwrap();
    /// ```
    /// This database will resume the sync settings that were provided when
    /// the database was created.
    pub fn open<P: AsRef<Path> + Clone + std::fmt::Debug>(path: P) -> Result<Self, DatabaseError> {
        info!("Opening Client with database at {:?}", path);
        if !path.as_ref().exists() {
            error!("Database does not exist exists, cannot open: {:?}", path);
            return Err(DatabaseError::DatabaseDoesNotExist(path.as_ref().to_str().unwrap().to_string()))
        } ;

        let mut f = open_file(&path)?;
        let mut compressed: Vec<u8> = Vec::new();
        f.read_to_end(&mut compressed)?;
        let uncompressed = decompress_size_prepended(&compressed)?;
        let database: Database = bincode::deserialize(&uncompressed)?;
        let sync_interval = database.sync_interval.clone();

        let mut client = Self{
            database: Arc::new(Mutex::new(database)),
            raw_file: Arc::new(Mutex::new(PathBuf::from(path.as_ref()))),
            handle: Arc::new(None),
        };

        if let Some(duration) = sync_interval {
            let mut c = client.clone();
            let (tx, rx) = std::sync::mpsc::sync_channel(0);
            let h = std::thread::spawn( move || loop {
                    if let Ok(_) = rx.try_recv() {
                        trace!("Breaking");
                        break
                    };
    
                    trace!("Sleeping for {:?}", duration);
                    sleep(duration);
                    
                    trace!("Pruning database");
                    c.prune().unwrap();
                    debug!("Database pruned");
                    
                    trace!("Saving database");
                    c.save().unwrap();
                    debug!("Database saved");
                }
            );
    
            client.handle = Arc::new(Some(Saver{
                handle: Some(h),
                killer: tx,
            }));
        };
        

        trace!("Returning Client");

        Ok(client)
    }

    /// Removes stale entries as defined by the expiration value per table
    /// and saves the database to disk; using lz4 compression
    /// ```
    /// # use persistent_keystore_rs::Client;
    /// use std::path::Path;
    /// let c = Client::new(Path::new("saved.db"), None);
    /// # std::fs::remove_file("saved.db").unwrap();
    /// ```
    /// This database will not sync on its own and will need to be saved with 
    /// ```
    /// # use persistent_keystore_rs::Client;
    /// # let mut c = Client::new(std::path::Path::new("saved2.db"), None).unwrap();
    /// c.save();
    /// # std::fs::remove_file("saved2.db").unwrap();
    pub fn save(&mut self) -> Result<(), DatabaseError> {
        trace!("Saving database");
        if let Ok(database) = self.database.lock() {
            if let Ok(raw_file) = self.raw_file.lock() {
                debug!("Saving database {:?}", raw_file);
                let mut f = OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create(true)
                    .truncate(true)
                    .append(false)
                    .open(raw_file.as_path())?;
                let output = bincode::serialize(&database.clone())?;
                let compressed = compress_prepend_size(&output);
                f.seek(SeekFrom::Start(0))?;
                f.write_all(&compressed)?;
                f.flush()?;
                f.sync_all()?;
                drop(f);
                return Ok(())

            } else {
                error!("Unable to get file mutex");
            };
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Creates a table within the database of the associated client
    /// ```
    /// use persistent_keystore_rs::{Client, Table, FieldType};
    /// use std::time::Duration;
    /// # use std::path::Path;
    /// let mut c = Client::new(Path::new("createtable.db"), None).unwrap();
    /// let table = Table::new()
    ///     .name(String::from("MyTable"))
    ///     .primary_field(FieldType::String).unwrap()
    ///     .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    ///     .add_expiration(Duration::from_secs(2592000))
    ///     .build().unwrap();
    /// c.create_table(table).unwrap();
    /// # std::fs::remove_file("createtable.db").unwrap();
    /// ```
    pub fn create_table(&mut self, table: Table) -> Result<(), DatabaseError> {
        trace!("Creating table {}", table.name);
        if let Ok(mut database) = self.database.lock() {
            match database.get_table(&table.name.clone()) {
                Ok(_) => {
                    error!("Table {} exists", table.name);
                    return Err(DatabaseError::TableExists(table.name))
                },
                Err(_) => {
                    debug!("Creating table {}", table.name);
                    return database.create_table(table)
                },
            };
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Lists tables within the database of the associated client
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType};
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// let mut c = Client::new(Path::new("listtable.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// let tables = c.list_tables().unwrap();
    /// assert_eq!(tables.len(), 1);
    /// assert_eq!(tables[0], String::from("MyTable"));
    /// # std::fs::remove_file("listtable.db").unwrap();
    /// ```
    pub fn list_tables(&mut self) -> Result<Vec<String>, DatabaseError> {
        trace!("Listing Tables");
        if let Ok(mut database) = self.database.lock() {
            let tables = database.list_tables();
            debug!("Listed {} tables", tables.len());
            return Ok(tables)
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Drops the specified table from within the database of the associated client
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType};
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// let mut c = Client::new(Path::new("droptable.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// let tables = c.list_tables().unwrap();
    /// assert_eq!(tables.len(), 1);
    /// assert_eq!(tables[0], String::from("MyTable"));
    /// c.drop_table(&String::from("MyTable")).unwrap();
    /// let current_tables = c.list_tables().unwrap();
    /// assert_eq!(current_tables.len(), 0);
    /// # std::fs::remove_file("droptable.db").unwrap();
    /// ```
    pub fn drop_table(&mut self, table: &String) -> Result<(), DatabaseError> {
        trace!("Dropping table {}", table);
        if let Ok(mut database) = self.database.lock() {
            debug!("Dropping table {}", table);
            return database.drop_table(table)
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Inserts the provided entry into the specified table within the database of the associated client.
    /// If an entry with the same primary key exists, an DatabaseError::EntryExists is returned
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType};
    /// use persistent_keystore_rs::{Entry, Field};
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// use std::time::SystemTime;
    /// let mut c = Client::new(Path::new("insertentry.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// let entry = Entry::new()
    ///     .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///     .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    ///     .build().unwrap();
    /// c.insert("MyTable".to_string(), entry).unwrap();
    /// # std::fs::remove_file("insertentry.db").unwrap();
    /// ```
    pub fn insert(&mut self, table: String, entry: Entry) -> Result<(), DatabaseError> {
        trace!("Inserting entry into table {}: {}", table, entry);
        if let Ok(mut database) = self.database.lock() {
            match database.get_table(&table) {
                Ok(t) => {
                    debug!("Inserting entry into table {}", table);
                    return t.insert(entry)
                },
                Err(_) => {
                    error!("Table {} does not exist", table);
                    return Err(DatabaseError::TableDoesNotExist(table))
                }
            }
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Inserts the provided entry into the specified table within the database of the associated client.
    /// If an entry with the same primary key exists, the entry is updated.
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType};
    /// use persistent_keystore_rs::{Entry, Field};
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// use std::time::SystemTime;
    /// let mut c = Client::new(Path::new("insertorupdateentry.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// let entry = Entry::new()
    ///     .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///     .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    ///     .build().unwrap();
    /// c.insert("MyTable".to_string(), entry.clone()).unwrap();
    /// c.insert_or_update("MyTable".to_string(), entry).unwrap();
    /// # std::fs::remove_file("insertorupdateentry.db").unwrap();
    /// ```
    pub fn insert_or_update(&mut self, table: String, entry: Entry) -> Result<(), DatabaseError> {
        trace!("Inserting or updating entry into table {}: {}", table, entry);
        if let Ok(mut database) = self.database.lock() {
            match database.get_table(&table) {
                Ok(t) => {
                    debug!("Inserting entry into table {}", table);
                    return t.insert_or_update(entry)
                },
                Err(_) => {
                    error!("Table {} does not exist", table);
                    return Err(DatabaseError::TableDoesNotExist(table))
                },
            }
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Updates an existing entry in the specified table within the database of the associated client.
    /// If an entry does not exist, DatabaseError::EntryDoesNotExists is returned
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType};
    /// use persistent_keystore_rs::{Entry, Field};
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// use std::time::SystemTime;
    /// let mut c = Client::new(Path::new("updateentry.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// let entry = Entry::new()
    ///     .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///     .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    ///     .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry.clone()).unwrap();
    /// c.update("MyTable".to_string(), entry).unwrap();
    /// # std::fs::remove_file("updateentry.db").unwrap();
    /// ```
    pub fn update(&mut self, table: String, entry: Entry) -> Result<(), DatabaseError> {
        trace!("Updating entry into table {}: {}", table, entry);
        if let Ok(mut database) = self.database.lock() {
            match database.get_table(&table) {
                Ok(t) => {
                    debug!("Updating entry {} in table {}", entry.primary_field, table);
                    return t.update(entry)
                },
                Err(_) => {
                    error!("Table {} does not exist", table);
                    return Err(DatabaseError::TableDoesNotExist(table))
                }
            }
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Get an existing entry from the specified table within the database of the associated client.
    /// If an entry does not exist, DatabaseError::EntryDoesNotExists is returned
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType, Entry};
    /// use persistent_keystore_rs::Field;
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// # use std::time::SystemTime;
    /// let mut c = Client::new(Path::new("getentry.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// # let entry = Entry::new()
    /// #    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry.clone()).unwrap();
    /// let e = c.get("MyTable".to_string(), Field::String("MyFirstEntry".to_string())).unwrap();
    /// # std::fs::remove_file("getentry.db").unwrap();
    /// ```
    pub fn get(&mut self, table: String, primary_field: Field) -> Result<Entry, DatabaseError> {
        trace!("Getting entry {} from table {}", primary_field, table);
        if let Ok(mut database) = self.database.lock() {
            match database.get_table(&table) {
                Ok(t) => {
                    debug!("Getting entry {} from table {}", primary_field, table);
                    let item = t.get(&primary_field)?;
                    return Ok(item.clone());
                },
                Err(_) => {
                    error!("Table {} does not exist", table);
                    return Err(DatabaseError::TableDoesNotExist(table))
                }
            }
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Delete an existing entry from the specified table within the database of the associated client.
    /// If an entry does not exist, DatabaseError::EntryDoesNotExists is returned
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType, Entry};
    /// use persistent_keystore_rs::Field;
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// # use std::time::SystemTime;
    /// let mut c = Client::new(Path::new("delete.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// # let entry = Entry::new()
    /// #    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry.clone()).unwrap();
    /// c.delete("MyTable".to_string(), Field::String("MyFirstEntry".to_string())).unwrap();
    /// # std::fs::remove_file("delete.db").unwrap();
    /// ```
    pub fn delete(&mut self, table: String, primary_field: Field) -> Result<(), DatabaseError> {
        trace!("Deleting entry {} from table {}", primary_field, table);
        if let Ok(mut database) = self.database.lock() {
            match database.get_table(&table) {
                Ok(t) => {
                    debug!("Deleting entry {} from table {}", primary_field, table);
                    return t.delete(primary_field)
                },
                Err(_) => {
                    error!("Table {} does not exist", table);
                    return Err(DatabaseError::TableDoesNotExist(table))
                },
            }
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Delete all entries matching the supplied criteria.
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType, Entry};
    /// use persistent_keystore_rs::Field;
    /// use std::collections::HashMap;
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// # use std::time::SystemTime;
    /// let mut c = Client::new(Path::new("deletemany.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_field(String::from("OptionalField"), FieldType::String).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// # let entry = Entry::new()
    /// #    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .add_field("OptionalField".to_string(), Field::String("MyTestingField".to_string())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry.clone()).unwrap();
    /// # let entry2 = Entry::new()
    /// #    .set_primary_field(Field::String("MySecondEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .add_field("OptionalField".to_string(), Field::String("MyTestingField".to_string())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry2.clone()).unwrap();
    /// # let entry3 = Entry::new()
    /// #    .set_primary_field(Field::String("MyThirdEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .add_field("OptionalField".to_string(), Field::String("MyOtherTestingField".to_string())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry3.clone()).unwrap();
    /// let mut criteria: HashMap<String, Field> = HashMap::new();
    /// criteria.insert("OptionalField".to_string(), Field::String("MyTestingField".to_string()));
    /// c.delete_many("MyTable".to_string(), criteria).unwrap();
    /// # assert_eq!(c.scan("MyTable".to_string()).unwrap().len(), 1);
    /// # std::fs::remove_file("deletemany.db").unwrap();
    /// ```
    pub fn delete_many(&mut self, table: String, criteria: HashMap<String, Field>) -> Result<u64, DatabaseError> {
        trace!("Deleting many from table {}", table);
        if let Ok(mut database) = self.database.lock() {
            match database.get_table(&table) {
                Ok(t) => {
                    let items = t.scan()?;
                    let mut deleted = 0;
                    'L:
                    for i in items {
                        for (k, v) in &criteria {
                            match &i.fields.get_key_value(k) {
                                Some((_, value)) => {
                                    if v != *value {
                                        trace!("{} does not meet criteria", i.primary_field);
                                        continue 'L;
                                    }
                                },
                                None => {
                                    trace!("{} does not meet criteria", i.primary_field);
                                    continue 'L
                                },
                            };
                            
                        };
                        debug!("Deleting entry {} from table {}", i.primary_field, table);
                        t.delete(i.primary_field)?;
                        deleted+=1;
                    };
                    return Ok(deleted)
                },
                Err(_) => {
                    error!("Table {} does not exist", table);
                    return Err(DatabaseError::TableDoesNotExist(table))
                },
            };
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Returns all entries from the specified table within the database of the associated client.
    /// If no entries exist, will return an empty vec
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType, Entry, Field};
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// # use std::time::SystemTime;
    /// let mut c = Client::new(Path::new("scan.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_field(String::from("OptionalField"), FieldType::String).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// # let entry = Entry::new()
    /// #    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .add_field("OptionalField".to_string(), Field::String("MyTestingField".to_string())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry.clone()).unwrap();
    /// # let entry2 = Entry::new()
    /// #    .set_primary_field(Field::String("MySecondEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .add_field("OptionalField".to_string(), Field::String("MyTestingField".to_string())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry2.clone()).unwrap();
    /// # let entry3 = Entry::new()
    /// #    .set_primary_field(Field::String("MyThirdEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .add_field("OptionalField".to_string(), Field::String("MyOtherTestingField".to_string())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry3.clone()).unwrap();
    /// let results = c.scan("MyTable".to_string()).unwrap();
    /// # assert_eq!(c.scan("MyTable".to_string()).unwrap().len(), 3);
    /// # std::fs::remove_file("scan.db").unwrap();
    /// ```
    pub fn scan(&mut self, table: String) -> Result<Vec<Entry>, DatabaseError> {
        trace!("Scanning table {}", table);
        if let Ok(mut database) = self.database.lock() {
            match database.get_table(&table) {
                Ok(t) => {
                    debug!("Scanning table {}", table);
                    return t.scan()
                },
                Err(_) => {
                    error!("Table {} does not exist", table);
                    return Err(DatabaseError::TableDoesNotExist(table))
                },
            };
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Query for entries within a specified table meeting the supplied criteria.
    /// ```
    /// # use persistent_keystore_rs::{Client, Table, FieldType, Entry};
    /// use persistent_keystore_rs::Field;
    /// use std::collections::HashMap;
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// # use std::time::SystemTime;
    /// let mut c = Client::new(Path::new("query.db"), None).unwrap();
    /// # let table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_field(String::from("OptionalField"), FieldType::String).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # c.create_table(table).unwrap();
    /// # let entry = Entry::new()
    /// #    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .add_field("OptionalField".to_string(), Field::String("MyTestingField".to_string())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry.clone()).unwrap();
    /// # let entry2 = Entry::new()
    /// #    .set_primary_field(Field::String("MySecondEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .add_field("OptionalField".to_string(), Field::String("MyTestingField".to_string())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry2.clone()).unwrap();
    /// # let entry3 = Entry::new()
    /// #    .set_primary_field(Field::String("MyThirdEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .add_field("OptionalField".to_string(), Field::String("MyOtherTestingField".to_string())).unwrap()
    /// #    .build().unwrap();
    /// # c.insert("MyTable".to_string(), entry3.clone()).unwrap();
    /// let mut criteria: HashMap<String, Field> = HashMap::new();
    /// criteria.insert("OptionalField".to_string(), Field::String("MyTestingField".to_string()));
    /// let results = c.query("MyTable".to_string(), criteria).unwrap();
    /// # assert_eq!(results.len(), 2);
    /// # std::fs::remove_file("query.db").unwrap();
    /// ```
    pub fn query(&mut self, table: String, criteria: HashMap<String, Field>) -> Result<Vec<Entry>, DatabaseError> {
        trace!("Querying table {}", table);
        if let Ok(mut database) = self.database.lock() {
            match database.get_table(&table) {
                Ok(t) => {
                    let items = t.scan()?;
                    let mut results = Vec::new();
                    'L:
                    for i in items {
                        for (k, v) in &criteria {
                            match &i.fields.get_key_value(k) {
                                Some((_, value)) => {
                                    if v != *value {
                                        trace!("{} does not meet criteria", i.primary_field);
                                        continue 'L;
                                    }
                                },
                                None => {
                                    trace!("{} does not meet criteria", i.primary_field);
                                    continue 'L;
                                },
                            };
                            
                        };
                        results.push(i.clone());
                    };
                    return Ok(results)
                },
                Err(_) => {
                    error!("Table {} does not exist", table);
                    return Err(DatabaseError::TableDoesNotExist(table))
                },
            };
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }

    /// Removes entries that have expired by the specified TTL field in the table.
    /// This is done automatically before saves if a sync_interval is provided.
    /// 
    /// ```
    /// use persistent_keystore_rs::{Client, Table, FieldType, Entry};
    /// # use std::thread::sleep;
    /// use persistent_keystore_rs::Field;
    /// use std::collections::HashMap;
    /// # use std::time::Duration;
    /// # use std::path::Path;
    /// # use std::time::SystemTime;
    /// let mut c = Client::new(Path::new("prune.db"), None).unwrap();
    /// let table = Table::new()
    ///     .name(String::from("MyTable"))
    ///     .add_expiration(Duration::from_secs(1))
    ///     .primary_field(FieldType::String).unwrap()
    ///     .add_field("FirstKey".to_string(), FieldType::I64).unwrap()
    ///     .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
    ///     .build().unwrap();
    ///
    /// c.create_table(table).unwrap();
    ///
    /// let entry_first = Entry::new()
    ///     .set_primary_field(Field::String("This should Succeed".to_string())).unwrap()
    ///     .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
    ///     .add_field("OptionalKey".to_string(), Field::String("My first entry".to_string())).unwrap()
    ///     .build().unwrap();
    ///
    /// let result = c.insert("MyTable".to_string(), entry_first);
    ///
    /// if let Err(e) = result {
    ///     panic!("No error expected, received {}", e)
    /// } 
    ///
    /// let output = c.scan("MyTable".to_string()).unwrap();
    ///
    /// if output.len() != 1 {
    ///     panic!("Expected scan results to be 1")
    /// };
    ///
    /// sleep(Duration::from_secs(2));
    /// c.prune().unwrap();
    ///
    /// let output = c.scan("MyTable".to_string()).unwrap();
    /// if output.len() != 0 {
    ///     panic!("Expected scan results to be 0")
    /// };
    /// # std::fs::remove_file("prune.db").unwrap();
    /// ```
    pub fn prune(&mut self) -> Result<(), DatabaseError> {
        trace!("Pruning database");
        if let Ok(mut database) = self.database.lock() {
            let current_time = SystemTime::now();
            for t in database.list_tables() {
                if let Ok(table) = database.get_table(&t) {
                    if let Some(expire_after) = table.expire_after {
                        let items = table.scan()?;
                        for item in items {
                            if let Some(n) = item.last_timestamp {
                                if let Ok(last_time) = current_time.duration_since(n) {
                                    if last_time > expire_after {
                                        debug!("Pruning item {}", item.clone());
                                        table.delete(item.primary_field)?;
                                    } else {
                                        trace!("Not yet expired {}", item.clone());
                                    }
                                } 
                            }
                        }
                    } else {
                        debug!("No expire after setting for table {}", t);
                    }
                }
            };
            return Ok(())
        };
        error!("Unable to get database lock");
        Err(DatabaseError::UnableToGetLock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;

    fn create_client_table(name: String) -> (Client, TableBuilder) {
        let mut temp_dir_path = temp_dir();
        temp_dir_path.push(format!("{}.db", name));
        if temp_dir_path.exists() {
            std::fs::remove_file(temp_dir_path.clone().to_str().unwrap()).unwrap();
        };

        let c = Client::new(temp_dir_path, None).unwrap();
        
        let table = structs::Table::new()
            .name(name);

        (c, table)
    }

    #[test]
    fn entry_missing_fields() {
        let missing_fields_entry = structs::Entry::new()
            .set_primary_field(Field::String("This entry should fail".to_string())).unwrap()
            .build();
        
        if let Err(e) = missing_fields_entry {
            match e {
                DatabaseError::EntryMustContainFields => {},
                _ => panic!("Expected EntryMustContainFields but not {}", e),
            };
        } else {
            panic!("Expected error and did not receive one");
        };
    }

    #[test]
    fn entry_mismatched_primary_field_type() {
        let (mut c, table_builder) = create_client_table("MismatchedPrimaryFieldType".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let mismatched_primary_field_type = structs::Entry::new()
            .set_primary_field(Field::I64(123123)).unwrap()
            .add_field("FirstKey".to_string(), Field::String("testing".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("MismatchedPrimaryFieldType".to_string(), mismatched_primary_field_type);

        if let Err(e) = result {
            match e {
                DatabaseError::MismatchedFieldType => {},
                _ => panic!("Expected MismatchedField type, not {}", e),
            };
        } else {
            panic!("Expected MismatchedFieldType error but received none");
        };
    }

    #[test]
    fn entry_mismatched_secondary_field_type() {
        let (mut c, table_builder) = create_client_table("MismatchedSecondaryFieldType".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let mismatched_optional_field = structs::Entry::new()
            .set_primary_field(Field::String("This should also fail".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::String("Should fail".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("MismatchedSecondaryFieldType".to_string(), mismatched_optional_field);

        if let Err(e) = result {
            match e {
                DatabaseError::MismatchedFieldType => {},
                _ => panic!("Expected MismatchedField type, not {}", e),
            };
        } else {
            panic!("Expected MismatchedFieldType error but received none");
        };
    }

    #[test]
    fn entry_extra_undefined_field() {
        let (mut c, table_builder) = create_client_table("EntryExtraUndefiniedField".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let extra_field = structs::Entry::new()
            .set_primary_field(Field::String("This should also fail".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My second field".to_string())).unwrap()
            .add_field("ExtraField".to_string(), Field::String("ExtraField".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("EntryExtraUndefiniedField".to_string(), extra_field);

        if let Err(e) = result {
            match e {
                DatabaseError::UnsupportedField(_) => {},
                _ => panic!("Expected UnsupportedField, got {}", e),
            };
        } else {
            panic!("Expected UnsupportedField, got none")
        }
    }

    #[test]
    fn entry_table_does_not_exist() {
        let (mut c, table_builder) = create_client_table("EntryTableDoesNotExist".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let extra_field = structs::Entry::new()
            .set_primary_field(Field::String("This should also fail".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My second field".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("EntryTableDoesNotExist2".to_string(), extra_field);

        if let Err(e) = result {
            match e {
                DatabaseError::TableDoesNotExist(_) =>  {},
                _ => panic!("Expected TableDoesNotExist, got {}", e),
            };
        } else {
            panic!("Expected TableDoesNotExist, got none")
        }
    }

    #[test]
    fn entry_success_without_optional() {
        let (mut c, table_builder) = create_client_table("EntrySuccessWithoutOptional".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let extra_field = structs::Entry::new()
            .set_primary_field(Field::String("This should Succeed".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .build().unwrap();

        let result = c.insert("EntrySuccessWithoutOptional".to_string(), extra_field);

        if let Err(e) = result {
            panic!("No error expected, received {}", e)
        } 
    }

    #[test]
    fn entry_success_with_optional() {
        let (mut c, table_builder) = create_client_table("EntrySuccessWithOptional".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let extra_field = structs::Entry::new()
            .set_primary_field(Field::String("This should Succeed".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My second field".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("EntrySuccessWithOptional".to_string(), extra_field);

        if let Err(e) = result {
            panic!("No error expected, received {}", e)
        } 
    }

    #[test]
    fn entry_update_success() {
        let (mut c, table_builder) = create_client_table("EntryUpdateSuccess".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let entry_first = structs::Entry::new()
            .set_primary_field(Field::String("This should Succeed".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My first entry".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("EntryUpdateSuccess".to_string(), entry_first);

        if let Err(e) = result {
            panic!("No error expected, received {}", e)
        } 

        let entry_second = structs::Entry::new()
            .set_primary_field(Field::String("This should Succeed".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My second entry".to_string())).unwrap()
            .build().unwrap();

        let result = c.update("EntryUpdateSuccess".to_string(), entry_second.clone());

        if let Err(e) = result {
            panic!("No error expected, received {}", e)
        }

        let curent_entry = c.get("EntryUpdateSuccess".to_string(), Field::String("This should Succeed".to_string())).unwrap();
        assert!(curent_entry.primary_field==entry_second.primary_field);
        assert!(curent_entry.fields==entry_second.fields);
    }

    #[test]
    fn entry_failure_second_insert() {
        let (mut c, table_builder) = create_client_table("SecondInsertFailure".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let entry_first = structs::Entry::new()
            .set_primary_field(Field::String("This should Succeed".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My first entry".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("SecondInsertFailure".to_string(), entry_first);

        if let Err(e) = result {
            panic!("No error expected, received {}", e)
        } 

        let entry_second = structs::Entry::new()
            .set_primary_field(Field::String("This should Succeed".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My second entry".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("SecondInsertFailure".to_string(), entry_second);

        if let Err(e) = result {
            match e {
                DatabaseError::EntryExists => {},
                _ => panic!("Expected EntryExists, got {}", e),
            };
        } else {
            panic!("Expected EntryExists, got none")
        }
    }

    #[test]
    fn database_open_close() {
        let (mut c, table_builder) = create_client_table("OpenSaveCloseOpen".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let entry_first = structs::Entry::new()
            .set_primary_field(Field::String("This should Succeed".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My first entry".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert_or_update("OpenSaveCloseOpen".to_string(), entry_first.clone());

        if let Err(e) = result {
            panic!("No error expected, received {}", e)
        } 

        c.save().unwrap();
        drop(c);

        let mut temp_dir_path = temp_dir();
        temp_dir_path.push("OpenSaveCloseOpen.db");

        let mut c = Client::open(temp_dir_path).unwrap();
        let curent_entry = c.get("OpenSaveCloseOpen".to_string(), Field::String("This should Succeed".to_string())).unwrap();
        assert!(curent_entry.primary_field==entry_first.primary_field);
        assert!(curent_entry.fields==entry_first.fields);
    }

    #[test]
    fn query_item() {
        let (mut c, table_builder) = create_client_table("QueryItems".to_string());
        
        let table = table_builder.primary_field(structs::FieldType::String).unwrap()
            .add_field("FirstKey".to_string(), structs::FieldType::I64).unwrap()
            .add_optional_field("OptionalKey".to_string(), FieldType::String).unwrap()
            .build().unwrap();

        c.create_table(table).unwrap();

        let entry_first = structs::Entry::new()
            .set_primary_field(Field::String("This should Succeed".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(123123)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My first entry".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("QueryItems".to_string(), entry_first.clone());

        if let Err(e) = result {
            panic!("No error expected, received {}", e)
        } 

        let entry_second = structs::Entry::new()
            .set_primary_field(Field::String("This should Succeed too".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(12312312)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My second entry".to_string())).unwrap()
            .build().unwrap();

        let result = c.insert("QueryItems".to_string(), entry_second.clone());

        if let Err(e) = result {
            panic!("No error expected, received {}", e)
        }

        let first_query = c.query("QueryItems".to_string(), HashMap::from_iter(vec![("OptionalKey".to_string(), Field::String("My first entry".to_string()))])).unwrap();
        let second_query = c.query("QueryItems".to_string(), HashMap::from_iter(vec![("FirstKey".to_string(),Field::I64(12312312))])).unwrap();
        
        assert!(first_query[0].primary_field==entry_first.primary_field);
        assert!(first_query[0].fields==entry_first.fields);
        assert!(first_query.len() == 1);

        assert!(second_query[0].primary_field==entry_second.primary_field);
        assert!(second_query[0].fields==entry_second.fields);
        assert!(second_query.len() == 1);
    }
}