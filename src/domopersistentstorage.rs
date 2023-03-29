use crate::domobroker::DomoBrokerConf;
use crate::domocache::DomoCacheElement;
use async_trait::async_trait;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rusqlite::{params, Connection, OpenFlags};
use std::error::Error;
use std::path::{Path, PathBuf};
use tokio_postgres::NoTls;

pub const SQLITE_MEMORY_STORAGE: &str = "<memory>";

#[async_trait]
pub trait DomoPersistentStorage {
    async fn store(&mut self, element: &DomoCacheElement);
    async fn get_all_elements(&mut self) -> Vec<DomoCacheElement>;
}

pub struct SqliteStorage {
    pub sqlite_file: PathBuf,
    pub sqlite_connection: Connection,
}

impl SqliteStorage {
    #[cfg(test)]
    pub fn new_in_memory() -> Self {
        Self::new_internal(SQLITE_MEMORY_STORAGE, true)
    }

    pub fn new(conf: &DomoBrokerConf) -> Self {
        return SqliteStorage::new_internal(conf.sqlite_file.clone(), conf.is_persistent_cache);
    }

    pub fn new_internal<P: AsRef<Path>>(sqlite_file: P, write_access: bool) -> Self {
        let conn_res = if sqlite_file.as_ref().to_str() == Some(SQLITE_MEMORY_STORAGE) {
            if !write_access {
                panic!("Can't open in-memory database read-only!");
            }
            Connection::open_in_memory()
        } else if !write_access {
            Connection::open_with_flags(&sqlite_file, OpenFlags::SQLITE_OPEN_READ_ONLY)
        } else {
            Connection::open(&sqlite_file)
        };

        let conn = match conn_res {
            Ok(conn) => conn,
            Err(e) => panic!("Error while opening the sqlite DB: {e:?}"),
        };
        if write_access {
            _ = conn
                .execute(
                    "CREATE TABLE IF NOT EXISTS domo_data (
                topic_name             TEXT,
                topic_uuid             TEXT,
                value                  TEXT,
                deleted                INTEGER,
                publication_timestamp   TEXT,
                publisher_peer_id       TEXT,
                PRIMARY KEY (topic_name, topic_uuid)
                )",
                    [],
                )
                .unwrap();
        }

        SqliteStorage {
            sqlite_file: sqlite_file.as_ref().to_path_buf(),
            sqlite_connection: conn,
        }
    }
}

#[async_trait]
impl DomoPersistentStorage for SqliteStorage {
    async fn store(&mut self, element: &DomoCacheElement) {
        let _ = match self.sqlite_connection.execute(
            "INSERT OR REPLACE INTO domo_data\
             (topic_name, topic_uuid, value, deleted, publication_timestamp, publisher_peer_id)\
              VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                element.topic_name,
                element.topic_uuid,
                element.value.to_string(),
                element.deleted,
                element.publication_timestamp.to_string(),
                element.publisher_peer_id
            ],
        ) {
            Ok(ret) => ret,
            Err(e) => panic!("Error while executing write operation on sqlite: {e:?}"),
        };
    }

    async fn get_all_elements(&mut self) -> Vec<DomoCacheElement> {
        // read all not deleted elements
        let mut stmt = self
            .sqlite_connection
            .prepare("SELECT * FROM domo_data")
            .unwrap();

        let values_iter = stmt
            .query_map([], |row| {
                let jvalue: String = row.get(2)?;
                let jvalue = serde_json::from_str(&jvalue);

                let pub_timestamp_string: String = row.get(4)?;

                Ok(DomoCacheElement {
                    topic_name: row.get(0)?,
                    topic_uuid: row.get(1)?,
                    value: jvalue.unwrap(),
                    deleted: row.get(3)?,
                    publication_timestamp: pub_timestamp_string.parse().unwrap(),
                    publisher_peer_id: row.get(5)?,
                    republication_timestamp: 0,
                })
            })
            .unwrap();

        values_iter.collect::<Result<Vec<_>, _>>().unwrap()
    }
}

pub struct PostgresStorage {
    house_table: String,
    client: tokio_postgres::Client,
}

impl PostgresStorage {
    pub async fn new(conf: &DomoBrokerConf) -> Result<Self, Box<dyn Error>> {
        // connection
        let connector = TlsConnector::builder().build()?;
        let connector = MakeTlsConnector::new(connector);

        let pg_url = "host=".to_owned()
            + &conf.db_url
            + " user="
            + &conf.db_user
            + " password="
            + &conf.db_password;
        //+ " sslmode=require";

        //let (client, _conn) =
        //    tokio_postgres::connect(&pg_url, connector).await?;

        let (client, connection) = tokio_postgres::connect(&pg_url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // create table if write_access = true
        if conf.is_persistent_cache {
            let command = "CREATE TABLE IF NOT EXISTS ".to_owned()
                + &conf.house_table
                + "(
                topic_name             VARCHAR(255),
                topic_uuid             VARCHAR(255),
                value                  VARCHAR(3000),
                deleted                INTEGER,
                publication_timestamp   TEXT,
                publisher_peer_id       VARCHAR(255),
                PRIMARY KEY (topic_name, topic_uuid)
                )";
            let ret = client.execute(&command, &[]).await;

            match ret {
                Ok(r) => {}
                Err(e) => {
                    println!("{e}")
                }
            }

            println!("Database table created");
        }

        Ok(Self {
            house_table: conf.house_table.to_string(),
            client,
        })
    }
}

#[async_trait]
impl DomoPersistentStorage for PostgresStorage {
    async fn store(&mut self, element: &DomoCacheElement) {
        let command = "INSERT INTO ".to_owned()
            + &self.house_table
            + "
             (topic_name, topic_uuid, value, deleted, publication_timestamp, publisher_peer_id)\
              VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT(topic_name, topic_uuid) DO UPDATE SET \
              value=$3, deleted=$4, publication_timestamp=$5, publisher_peer_id=$6";
        let _ret = self
            .client
            .execute(
                &command,
                &[
                    &element.topic_name,
                    &element.topic_uuid,
                    &element.value.to_string(),
                    &i32::from(element.deleted),
                    &element.publication_timestamp.to_string(),
                    &element.publisher_peer_id,
                ],
            )
            .await;

        if let Err(_ret) = _ret {
            println!("{_ret}");
        }
    }

    async fn get_all_elements(&mut self) -> Vec<DomoCacheElement> {
        let mut v = Vec::new();
        let command = "SELECT * FROM ".to_owned() + &self.house_table;
        let ret = self.client.query(&command, &[]).await;
        if let Ok(r) = ret {
            for row in r {
                let jvalue: String = row.get(2);
                let jvalue = serde_json::from_str(&jvalue);
                let pub_timestamp_string: String = row.get(4);

                let del: i32 = row.get(3);
                let deleted = (del == 1);

                v.push(DomoCacheElement {
                    topic_name: row.get(0),
                    topic_uuid: row.get(1),
                    value: jvalue.unwrap(),
                    deleted: deleted,
                    publication_timestamp: pub_timestamp_string.parse().unwrap(),
                    publisher_peer_id: row.get(5),
                    republication_timestamp: 0,
                });
            }
        }
        v
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[should_panic]
    fn open_read_from_memory() {
        let _s = super::SqliteStorage::new_internal(super::SQLITE_MEMORY_STORAGE, false);
    }

    #[test]
    #[should_panic]
    fn open_read_non_existent_file() {
        let _s = super::SqliteStorage::new_internal("aaskdjkasdka.sqlite", false);
    }

    #[test]
    fn open_write_new_file() {
        let s = super::SqliteStorage::new_in_memory();
        assert_eq!(s.sqlite_file.to_str(), Some(super::SQLITE_MEMORY_STORAGE));
    }

    #[tokio::test]
    async fn test_initial_get_all_elements() {
        use super::DomoPersistentStorage;

        let mut s = crate::domopersistentstorage::SqliteStorage::new_in_memory();
        let v = s.get_all_elements().await;
        assert_eq!(v.len(), 0);
    }

    #[tokio::test]
    async fn test_store() {
        use super::DomoPersistentStorage;
        let mut s = crate::domopersistentstorage::SqliteStorage::new_in_memory();

        let m = crate::domocache::DomoCacheElement {
            topic_name: "a".to_string(),
            topic_uuid: "a".to_string(),
            value: Default::default(),
            deleted: false,
            publication_timestamp: 0,
            publisher_peer_id: "a".to_string(),
            republication_timestamp: 0,
        };

        s.store(&m).await;

        let v = s.get_all_elements().await;

        assert_eq!(v.len(), 1);
        assert_eq!(v[0], m);
    }
}
