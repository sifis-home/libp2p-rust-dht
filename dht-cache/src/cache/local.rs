//! Local in-memory cache

pub use crate::data::*;
use crate::domopersistentstorage::{DomoPersistentStorage, SqlxStorage};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::{btree_map, BTreeMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::{OwnedRwLockReadGuard, RwLock, RwLockReadGuard};

enum SqlxCommand {
    Write(DomoCacheElement),
}

#[derive(Debug, Default)]
pub(crate) struct InnerCache {
    pub mem: BTreeMap<String, BTreeMap<String, DomoCacheElement>>,
    store: Option<UnboundedSender<SqlxCommand>>,
}

impl InnerCache {
    pub fn put(&mut self, elem: DomoCacheElement) {
        let topic_name = &elem.topic_name;
        let topic_uuid = elem.topic_uuid.to_owned();

        if let Some(topic) = self.mem.get_mut(topic_name) {
            topic.insert(topic_uuid, elem);
        } else {
            self.mem.insert(
                topic_name.to_owned(),
                [(topic_uuid.to_owned(), elem)].into(),
            );
        }
    }
}

/// Local cache
#[derive(Default, Clone)]
pub struct LocalCache(Arc<RwLock<InnerCache>>);

impl LocalCache {
    /// Instantiate a local cache from the configuration provided
    ///
    /// If url is empty do not try to bootstrap the cache from the db
    ///
    /// TODO: propagate errors
    pub async fn with_config(db_config: &sifis_config::Cache) -> Self {
        let mut inner = InnerCache::default();

        if !db_config.url.is_empty() {
            let mut store = SqlxStorage::new(db_config).await;

            for a in store.get_all_elements().await {
                inner.put(a);
            }

            if db_config.persistent {
                let (s, mut r) = unbounded_channel();

                tokio::task::spawn(async move {
                    while let Some(SqlxCommand::Write(elem)) = r.recv().await {
                        store.store(&elem).await
                    }
                    panic!("I'm out!");
                });
                inner.store = Some(s);
            }
        }

        LocalCache(Arc::new(RwLock::new(inner)))
    }

    pub fn new() -> Self {
        Default::default()
    }

    /// Feeds a slice of this type into the given [`Hasher`].
    pub async fn hash<H: Hasher>(&self, state: &mut H) {
        let cache = &self.0.read().await.mem;
        for (topic_name, map_topic_name) in cache.iter() {
            topic_name.hash(state);

            for (topic_uuid, value) in map_topic_name.iter() {
                topic_uuid.hash(state);
                value.to_string().hash(state);
            }
        }
    }

    /// Put the element in the cache
    ///
    /// If it is already present overwrite it
    pub async fn put(&self, elem: DomoCacheElement) {
        let mut cache = self.0.write().await;

        if let Some(s) = cache.store.as_mut() {
            let _ = s.send(SqlxCommand::Write(elem.to_owned()));
        }

        cache.put(elem);
    }

    /// Try to insert the element in the cache
    ///
    /// Return false if the element to insert is older than the one in the cache
    pub async fn try_put(&self, elem: DomoCacheElement) -> bool {
        let mut cache = self.0.write().await;
        let topic_name = elem.topic_name.clone();
        let topic_uuid = &elem.topic_uuid;

        let topic = cache.mem.entry(topic_name).or_default();

        let e = if topic
            .get(topic_uuid)
            .is_some_and(|cur| elem.publication_timestamp <= cur.publication_timestamp)
        {
            false
        } else {
            topic.insert(topic_uuid.to_owned(), elem.clone());
            true
        };

        if e {
            if let Some(s) = cache.store.as_mut() {
                let _ = s.send(SqlxCommand::Write(elem));
            }
        }

        e
    }

    /// Retrieve an element by its uuid and topic
    pub async fn get(&self, topic_name: &str, topic_uuid: &str) -> Option<DomoCacheElement> {
        let cache = self.0.read().await;

        cache
            .mem
            .get(topic_name)
            .and_then(|topic| topic.get(topic_uuid))
            .cloned()
    }

    /// Instantiate a query over the local cache
    pub fn query<'a>(&self, topic: &'a str) -> Query<'a> {
        Query::new(topic, self.clone())
    }

    /// Compute the current hash value
    pub async fn get_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s).await;
        s.finish()
    }

    pub(crate) async fn read_owned(&self) -> OwnedRwLockReadGuard<InnerCache> {
        self.0.clone().read_owned().await
    }
}

/// Query the local DHT cache
#[derive(Clone)]
pub struct Query<'a> {
    cache: LocalCache,
    topic: &'a str,
}

impl<'a> Query<'a> {
    /// Create a new query over a local cache
    pub fn new(topic: &'a str, cache: LocalCache) -> Self {
        Self { topic, cache }
    }

    /// Gets a value on a topic given a specific UUID.
    ///
    /// Keep in mind that the returned type holds a lock guard to the underlying data, be careful
    /// to use it across yield points.
    pub async fn get_by_uuid<'b>(&'b self, uuid: &'b str) -> Option<RwLockReadGuard<'b, Value>> {
        RwLockReadGuard::try_map(self.cache.0.read().await, |cache| {
            cache
                .mem
                .get(self.topic)
                .and_then(|tree| tree.get(uuid))
                .map(|cache_element| &cache_element.value)
        })
        .ok()
    }

    /// Gets the data stored for the topic.
    ///
    /// It returns an _iterable type_ that can be used to obtain pairs of UUID and values.
    ///
    /// Keep in mind that the returned type holds a lock guard to the underlying data, be careful
    /// to use it across yield points.
    ///
    /// # Example
    ///
    /// ```
    /// # use sifis_dht::cache::Query;
    /// # async fn handle_query(query: Query<'_>) {
    /// let get = query.get().await;
    /// for pair in &get {
    ///     let (uuid, value): (&str, &serde_json::Value) = pair;
    ///     println!("{uuid}, {value}");
    /// }
    /// # }
    /// ```
    #[inline]
    pub async fn get(&self) -> QueryGet<'_> {
        let lock =
            RwLockReadGuard::try_map(self.cache.0.read().await, |cache| cache.mem.get(self.topic))
                .ok();

        QueryGet(lock)
    }
}

#[derive(Debug)]
pub struct QueryGet<'a>(Option<RwLockReadGuard<'a, BTreeMap<String, DomoCacheElement>>>);

impl<'a> QueryGet<'a> {
    /// Iterate over queried pairs of UUIDs and values.
    #[inline]
    #[must_use]
    pub fn iter(&'a self) -> QueryGetIter<'a> {
        IntoIterator::into_iter(self)
    }
}

impl<'a> IntoIterator for &'a QueryGet<'a> {
    type Item = (&'a str, &'a Value);
    type IntoIter = QueryGetIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        let values = self
            .0
            .as_deref()
            .map_or_else(Default::default, BTreeMap::iter);

        QueryGetIter(values)
    }
}

#[derive(Debug)]
pub struct QueryGetIter<'a>(btree_map::Iter<'a, String, DomoCacheElement>);

impl<'a> Iterator for QueryGetIter<'a> {
    type Item = (&'a str, &'a Value);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(uuid, cache_element)| (&**uuid, &cache_element.value))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl DoubleEndedIterator for QueryGetIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|(uuid, cache_element)| (&**uuid, &cache_element.value))
    }
}

impl ExactSizeIterator for QueryGetIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data::DomoCacheElement;
    use serde_json::*;

    fn make_test_element(topic_name: &str, topic_uuid: &str, value: &Value) -> DomoCacheElement {
        DomoCacheElement {
            topic_name: topic_name.to_owned(),
            topic_uuid: topic_uuid.to_owned(),
            value: value.to_owned(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn hash() {
        let cache = LocalCache::new();

        let hash = cache.get_hash().await;
        println!("{hash}");

        let elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": true}));
        cache.put(elem).await;

        let hash2 = cache.get_hash().await;
        println!("{hash2}");

        assert_ne!(hash, hash2);

        let elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": false}));
        cache.put(elem).await;

        let hash3 = cache.get_hash().await;
        println!("{hash3}");

        assert_ne!(hash2, hash3);

        let elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": true}));
        cache.put(elem).await;

        let hash4 = cache.get_hash().await;
        println!("{hash4}");

        assert_eq!(hash2, hash4);
    }

    #[tokio::test]
    async fn put() {
        let cache = LocalCache::new();

        let elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": true}));

        cache.put(elem.clone()).await;

        let out = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out, elem);

        let elem2 = make_test_element("Domo::Light", "luce-1", &json!({ "connected": false}));

        cache.put(elem2.clone()).await;

        let out = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out, elem2);
    }

    #[tokio::test]
    async fn try_put() {
        let cache = LocalCache::new();

        let mut elem = make_test_element("Domo::Light", "luce-1", &json!({ "connected": true}));

        assert!(cache.try_put(elem.clone()).await);

        let out = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out, elem);

        elem.publication_timestamp = 1;

        assert!(cache.try_put(elem.clone()).await);

        let out: DomoCacheElement = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out, elem);

        elem.publication_timestamp = 0;

        assert!(!cache.try_put(elem).await);

        let out: DomoCacheElement = cache.get("Domo::Light", "luce-1").await.expect("element");

        assert_eq!(out.publication_timestamp, 1);
    }

    #[tokio::test]
    async fn query() {
        let cache = LocalCache::new();

        for item in 0..10 {
            let elem = make_test_element(
                "Domo::Light",
                &format!("luce-{item}"),
                &json!({ "connected": true, "count": item}),
            );

            cache.put(elem).await;
        }

        let q = cache.query("Domo::Light");

        assert_eq!(q.get().await.iter().len(), 10);

        assert!(q.get_by_uuid("not-existent").await.is_none());

        assert_eq!(
            q.clone()
                .get_by_uuid("luce-1")
                .await
                .unwrap()
                .get("count")
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn persistence() {
        let cfg = crate::Config {
            ..Default::default()
        };

        let cache = LocalCache::with_config(&cfg).await;

        for item in 0..10 {
            let elem = make_test_element(
                "Domo::Light",
                &format!("luce-{item}"),
                &json!({ "connected": true, "count": item}),
            );

            cache.put(elem).await;
        }
    }
}
