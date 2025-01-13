use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};
use serde::{Deserialize, Serialize};
use rmp_serde::decode::from_read;
use uuid::Uuid;
use std::thread;
use flate2::read::GzDecoder;
use rand::{thread_rng, seq::SliceRandom};

#[derive(Serialize, Deserialize, Clone)]
struct Record {
    pub id: String,
    pub name: String,
    pub age: u32,
    pub email: String,
}

struct Database {
    path: String,
    data: Arc<Mutex<HashMap<String, Record>>>,
    indexes: Arc<Mutex<HashMap<String, HashMap<String, HashSet<String>>>>>,
}

impl Database {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            data: Arc::new(Mutex::new(HashMap::new())),
            indexes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn init(&self) -> std::io::Result<()> {
        // Load existing data
        self.load()?;
        // Rebuild indexes
        self.build_indexes();
        Ok(())
    }

    fn load(&self) -> std::io::Result<()> {
        if !Path::new(&self.path).exists() {
            return Ok(());
        }
        let file = File::open(&self.path)?;
        let decoder = GzDecoder::new(file);
        if let Ok(records) = from_read(decoder) {
            let records: Vec<Record> = records;
            let mut data_guard = self.data.lock().unwrap();
            for record in records {
                data_guard.insert(record.id.clone(), record);
            }
        }
        Ok(())
    }

    pub async fn save(&self) -> std::io::Result<()> {
        let data_guard = self.data.lock().unwrap();
        let records: Vec<_> = data_guard.values().cloned().collect();
        drop(data_guard);

        tokio::task::spawn_blocking({
            let path = self.path.clone();
            move || -> std::io::Result<()> {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&path)?;
                rmp_serde::encode::write(&mut file, &records).unwrap();
                Ok(())
            }
        })
        .await??;

        Ok(())
    }

    fn build_indexes(&self) {
        let data_snapshot = {
            let guard = self.data.lock().unwrap();
            guard.values().cloned().collect::<Vec<_>>()
        };

        // Skip if no data
        if data_snapshot.is_empty() {
            return;
        }

        let chunk_count = 4; // number of threads


        let chunk_size = std::cmp::max(1, 
            (data_snapshot.len() as f64 / chunk_count as f64).ceil() as usize
        );
        let mut handles = vec![];

        for chunk in data_snapshot.chunks(chunk_size) {
            let chunk = chunk.to_vec();
            let indexes_ref = Arc::clone(&self.indexes);
            handles.push(thread::spawn(move || {
                let mut local_index: HashMap<String, HashMap<String, HashSet<String>>> = HashMap::new();
                // Build partial indexes
                for record in chunk {
                    for (key, value) in [
                        ("name", record.name.clone()),
                        ("age", record.age.to_string()),
                        ("email", record.email.clone()),
                    ] {
                        local_index.entry(key.to_string())
                            .or_insert_with(HashMap::new)
                            .entry(value)
                            .or_insert_with(HashSet::new)
                            .insert(record.id.clone());
                    }
                }
                // Merge partial indexes
                let mut idx_guard = indexes_ref.lock().unwrap();
                for (k, v) in local_index {
                    let entry = idx_guard.entry(k).or_insert_with(HashMap::new);
                    for (val, set_ids) in v {
                        entry.entry(val).or_insert_with(HashSet::new).extend(set_ids);
                    }
                }
            }));
        }
        // Waiting for threads
        for h in handles {
            let _ = h.join();
        }
    }

    pub fn create(&self, mut record: Record) {
        if record.id.is_empty() {
            record.id = Uuid::new_v4().to_string();
        }
        let mut data_guard = self.data.lock().unwrap();
        data_guard.insert(record.id.clone(), record);
    }

    pub fn read_all(&self) -> Vec<Record> {
        let data_guard = self.data.lock().unwrap();
        data_guard.values().cloned().collect()
    }

    pub fn update(&self, id: &str, updates: &Record) -> Option<Record> {
        let mut data_guard = self.data.lock().unwrap();
        if let Some(existing) = data_guard.get_mut(id) {
            existing.name = updates.name.clone();
            existing.age = updates.age;
            existing.email = updates.email.clone();
            return Some(existing.clone());
        }
        None
    }

    pub fn find_by_key(&self, key: &str, value: &str) -> Vec<Record> {
        let idx_guard = self.indexes.lock().unwrap();
        if let Some(val_map) = idx_guard.get(key) {
            if let Some(id_set) = val_map.get(value) {
                let data_guard = self.data.lock().unwrap();
                return id_set
                    .iter()
                    .filter_map(|id| data_guard.get(id).cloned())
                    .collect();
            }
        }
        vec![]
    }

    pub fn delete(&self, id: &str) -> bool {
        let mut data_guard = self.data.lock().unwrap();
        data_guard.remove(id).is_some()
    }

    fn run_random_queries(&self, count: usize) {
        let mut rng = thread_rng();
        let queries = [
            ("age", (0..100).map(|n| n.to_string()).collect::<Vec<_>>()), 
            ("name", (0..10).map(|n| format!("User{}", n * 1000)).collect()), 
            ("email", (0..10).map(|n| format!("user{}@example.com", n * 1000)).collect()),
        ];

        for i in 0..count {
            let (field, values) = queries.choose(&mut rng).unwrap();
            let value = values.choose(&mut rng).unwrap();
            let query_start = Instant::now();
            let results = self.find_by_key(field, value);
            println!(
                "Query #{}: {}={} ({} results): {:?}", 
                i + 1, field, value, results.len(), query_start.elapsed()
            );
        }
    }

    // benchmark
    pub async fn run_benchmark(&self) -> std::io::Result<()> {
        let entries = 1_000_000;
        let create_start = Instant::now();
        for i in 0..entries {
            self.create(Record {
                id: "".to_string(),
                name: format!("User{}", i),
                age: (i % 100) as u32, // age is created randomly
                email: format!("user{}@example.com", i),
            });
        }
        println!("Create {} entries: {:?}", entries, create_start.elapsed());

        // Rebuild indexes after creation
        let index_start = Instant::now();
        self.build_indexes();
        println!("Build indexes: {:?}", index_start.elapsed());

        let read_start = Instant::now();
        let data = self.read_all();
        println!("Read {} entries: {:?}", data.len(), read_start.elapsed());

        // Now queries will work since indexes are built
        let query_start = Instant::now();
        let results = self.find_by_key("age", "25");
        println!(
            "Query by age=25 ({} results): {:?}", 
            results.len(), 
            query_start.elapsed()
        );

        println!("\n=== Running 10 Random Queries ===");
        let random_queries_start = Instant::now();
        self.run_random_queries(10);
        println!("Total random queries time: {:?}", random_queries_start.elapsed());

        let update_start = Instant::now();
        for item in data.iter().take(2000) {
            let mut updated = item.clone();
            updated.age += 1;
            self.update(&item.id, &updated);
        }
        println!("Batch update 2000 entries: {:?}", update_start.elapsed());

        // Rebuild indexes after updates
        let reindex_start = Instant::now();
        self.build_indexes();
        println!("Rebuild indexes after update: {:?}", reindex_start.elapsed());

        let delete_start = Instant::now();
        for item in data.iter().take(2000) {
            self.delete(&item.id);
        }
        println!("Batch delete 2000 entries: {:?}", delete_start.elapsed());

        // Final index rebuild after deletions
        self.build_indexes();

        let save_start = Instant::now();
        self.save().await?;
        println!("Save to disk: {:?}", save_start.elapsed());
        
        println!("=== Benchmark Complete ===");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let db = Database::new("rust_benchmark_data.pookie");
    match db.init() {
        Ok(_) => {
            if let Err(e) = db.run_benchmark().await {
                eprintln!("Benchmark failed: {}", e);
            }
        },
        Err(e) => eprintln!("Failed to init database: {}", e),
    }
}
