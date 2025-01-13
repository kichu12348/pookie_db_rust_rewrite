use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::Path,
    time::Instant,
    io::Write,
    fs::OpenOptions,
    time::Duration,
    sync::atomic::{AtomicUsize, Ordering},
};
//deps
use rayon::prelude::*;
use std::sync::{Arc, Mutex};
use std::num::NonZeroUsize;
use serde::{Deserialize, Serialize};
use rmp_serde::decode::from_read;
use parking_lot::RwLock;
use uuid::Uuid;
use rand::{thread_rng, seq::SliceRandom};
use memmap2::MmapOptions;
use lz4_flex::decompress_size_prepended;
use sysinfo::{System, SystemExt};
use num_cpus;
use std::io::{self, BufRead};

//ignore non used code 💀👀 heh
#[allow(dead_code)]
const DEFAULT_CHUNK_SIZE: usize = 250_000; 
const INDEX_SHARD_SIZE: usize = 10_000;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Record {
    pub id: String, 
    pub name: String, 
    pub age: u32,
    pub email: String
}

struct QueryCache {
    results: HashMap<String, (Vec<Record>, Instant)>,
    max_size: usize,
    ttl: Duration,
    hits: AtomicUsize,
    misses: AtomicUsize,
}

impl QueryCache {
    fn new(max_size: usize, ttl_secs: u64) -> Self {
        Self {
            results: HashMap::new(),
            max_size,
            ttl: Duration::from_secs(ttl_secs),
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }
}

struct Database {
    path: String,
    data: Arc<RwLock<HashMap<String, Record>>>,
    indexes: Arc<RwLock<HashMap<String, Vec<HashMap<String, HashSet<String>>>>>>, // Changed to sharded structure
    batch_size: NonZeroUsize,
    shard_count: usize,
    query_cache: Arc<RwLock<QueryCache>>,
    index_stats: Arc<RwLock<HashMap<String, HashMap<String, usize>>>>,
}

impl Database {
    // Add batch size tuning based on system memory
    pub fn new(path: &str) -> Self {
        let optimal_batch_size = {
            let mut sys = System::new_all();
            sys.refresh_memory();
            let total_mem = sys.total_memory(); // Get memory in KB
            let mem_mb = total_mem / 1024;
            NonZeroUsize::new(((mem_mb as f64 * 0.1) as usize).max(100_000).min(500_000)).unwrap()
        };

        Self {
            path: path.to_string(),
            data: Arc::new(RwLock::new(HashMap::with_capacity(1_000_000))),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            batch_size: optimal_batch_size,
            shard_count: num_cpus::get().max(8), // Dynamic shard count based on CPU cores
            query_cache: Arc::new(RwLock::new(QueryCache::new(1000, 300))), // 1000 entries, 5 min TTL
            index_stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn init(&self) -> std::io::Result<()> {
        let meta_path = format!("{}.meta", self.path);
        
        if Path::new(&meta_path).exists() {
            println!("Found database metadata file");
            let metadata = std::fs::read_to_string(&meta_path)?;
            let mut lines = metadata.lines();
            
            if let (Some(chunk_count_str), Some(total_records_str)) = (lines.next(), lines.next()) {
                let chunk_count: usize = chunk_count_str.parse().unwrap_or(0);
                let total_records: usize = total_records_str.parse().unwrap_or(0);
                
                if Path::new(&format!("{}.0", self.path)).exists() {
                    println!("Loading {} records from {} chunks", total_records, chunk_count);
                    
                    // Load all chunks in parallel
                    let mut futures = Vec::new();
                    for chunk_id in 0..chunk_count {
                        let chunk_path = self.path.clone();
                        futures.push(Database::load_chunk(chunk_path, chunk_id));
                    }
                    
                    // Wait for all chunks to load
                    let chunks_results = futures::future::join_all(futures).await;
                    
                    // Process results
                    let mut data_guard = self.data.write();
                    data_guard.reserve(total_records);
                    
                    for chunk_result in chunks_results {
                        match chunk_result {
                            Ok(records) => {
                                println!("Processing chunk with {} records", records.len());
                                for record in records {
                                    data_guard.insert(record.id.clone(), record);
                                }
                            },
                            Err(e) => {
                                eprintln!("Error loading chunk: {}", e);
                            }
                        }
                    }
                    drop(data_guard);
                    
                    println!("Building indexes...");
                    self.build_indexes_full();
                    println!("Database load complete");
                    return Ok(());
                }
            }
        }

        // Fallback to checking for single file
        if Path::new(&self.path).exists() {
            self.load()?;
            self.build_indexes_full();
            return Ok(());
        }

        println!("No existing database files found at {}", self.path);
        Ok(())
    }

    fn load(&self) -> std::io::Result<()> {
        if !Path::new(&self.path).exists() {
            println!("No existing database file found at {}", self.path);
            return Ok(());
        }

        let file = File::open(&self.path)?;
        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            println!("Empty database file found");
            return Ok(());
        }

        println!("Loading database from {} (size: {} bytes)", self.path, metadata.len());
        
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        if mmap.len() < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File too small to contain size prefix"
            ));
        }

        let size_bytes = &mmap[..4];
        let expected_size = u32::from_le_bytes(size_bytes.try_into().unwrap()) as usize;
        println!("Expected decompressed size: {} bytes", expected_size);

       
        let buffer_size = expected_size + 4096;
        let decompressed;  
        
        println!("Attempting to decompress {} bytes with buffer size {}", mmap.len(), buffer_size);
        
       
        let decompression_result = decompress_size_prepended(&mmap);
        
        match decompression_result {
            Ok(data) => {
                if data.len() >= expected_size {
                    decompressed = data;
                    println!("Successfully decompressed to {} bytes", decompressed.len());
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Decompressed size too small: got {}, expected {}", data.len(), expected_size)
                    ));
                }
            },
            Err(e) => {
                eprintln!("Primary decompression failed: {}. Trying fallback...", e);
                // Fallback decompression with larger buffer
                let compressed_data = &mmap[4..];
                match lz4_flex::decompress(compressed_data, buffer_size) {
                    Ok(data) => {
                        if data.len() >= expected_size {
                            decompressed = data;
                            println!("Successfully decompressed using fallback to {} bytes", decompressed.len());
                        } else {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Fallback decompression size too small: got {}, expected {}", data.len(), expected_size)
                            ));
                        }
                    },
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Both decompression methods failed. Last error: {}", e)
                        ));
                    }
                }
            }
        }

        // Validate and deserialize
        if decompressed.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Decompressed data is empty"
            ));
        }

        match from_read::<_, Vec<Record>>(&decompressed[..]) {
            Ok(records) => {
                println!("Successfully deserialized {} records", records.len());
                let mut data_guard = self.data.write();
                for record in records {
                    data_guard.insert(record.id.clone(), record);
                }
                Ok(())
            }
            Err(e) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Deserialization error: {}", e)
                ))
            }
        }
    }

    // Add memory-aware save chunking
    pub async fn save(&self) -> std::io::Result<()> {
        let records: Vec<Record> = {
            let data_guard = self.data.read();
            data_guard.values().cloned().collect()
        };

        if records.is_empty() {
            println!("No records to save");
            return Ok(());
        }

        println!("Attempting to save {} records in chunks", records.len());
        
        let mut sys = System::new_all();
        sys.refresh_memory();
        let total_mem = sys.total_memory() / 1024; // Convert to MB
        let chunk_size = ((total_mem as f64 * 0.05) as usize).max(100_000).min(self.batch_size.get());
        let path = self.path.clone();
        
        // Clean up old chunks first
        let base_path = self.path.clone();
        if let Ok(entries) = std::fs::read_dir(std::path::Path::new(&base_path).parent().unwrap_or(std::path::Path::new("."))) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.to_string_lossy().contains(&base_path) && path.to_string_lossy().contains(".") {
                    let _ = std::fs::remove_file(path);
                }
            }
        }

        // Save main index file with chunk information
        let chunk_count = (records.len() + chunk_size - 1) / chunk_size;
        let mut save_handles = Vec::new();
        
        for (chunk_id, chunk) in records.chunks(chunk_size).enumerate() {
            let chunk_vec = chunk.to_vec();
            let chunk_path = path.clone();
            save_handles.push(tokio::spawn(async move {
                Database::save_chunk(chunk_vec, chunk_path, chunk_id).await
            }));
        }

        // Wait for all chunks to be saved
        for handle in save_handles {
            handle.await??;
        }

        // Save metadata about chunks
        let metadata = format!("{}\n{}", chunk_count, records.len());
        std::fs::write(format!("{}.meta", path), metadata)?;
        
        println!("Successfully saved database in {} chunks", chunk_count);
        Ok(())
    }

    async fn load_chunk(path: String, chunk_id: usize) -> std::io::Result<Vec<Record>> {
        let chunk_path = format!("{}.{}", path, chunk_id);
        println!("Loading chunk from {}", chunk_path);
        
        let file = File::open(&chunk_path)?;
        let metadata = file.metadata()?;
        println!("Chunk {} size: {} bytes", chunk_id, metadata.len());
        
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        if mmap.len() < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Chunk file too small"
            ));
        }

        let decompressed = decompress_size_prepended(&mmap)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Decompression error: {}", e)))?;
        
        println!("Chunk {} decompressed size: {} bytes", chunk_id, decompressed.len());
        
        let records = from_read(&decompressed[..])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Deserialization error: {}", e)))?;
            
        println!("Chunk {} loaded successfully", chunk_id);
        Ok(records)
    }

    fn build_indexes_full(&self) {
        let data_snapshot: Vec<_> = {
            let guard = self.data.read();
            guard.values().cloned().collect()
        };
        if data_snapshot.is_empty() {
            return;
        }
        let chunk_size = self.batch_size.get();
        let mut handles = vec![];
        let shard_count = self.shard_count;

        for chunk in data_snapshot.chunks(chunk_size) {
            let chunk = chunk.to_vec();
            let indexes_ref = Arc::clone(&self.indexes);
            handles.push(std::thread::spawn(move || {
                let mut local_indexes: HashMap<String, Vec<HashMap<String, HashSet<String>>>> = HashMap::new();
                
                for record in chunk.iter() {
                    let shard_id = record.id.as_bytes()[0] as usize % shard_count;
                    for (key, value) in [
                        ("name", record.name.clone()),
                        ("age", record.age.to_string()),
                        ("email", record.email.clone()),
                    ] {
                        let index_shards = local_indexes
                            .entry(key.to_string())
                            .or_insert_with(|| vec![HashMap::new(); shard_count]);
                        
                        index_shards[shard_id]
                            .entry(value)
                            .or_insert_with(HashSet::new)
                            .insert(record.id.clone());
                    }
                }

                let mut idx_guard = indexes_ref.write();
                for (key, shards) in local_indexes {
                    let global_shards = idx_guard
                        .entry(key)
                        .or_insert_with(|| vec![HashMap::new(); shard_count]);
                    
                    for (shard_id, local_shard) in shards.into_iter().enumerate() {
                        for (value, ids) in local_shard {
                            global_shards[shard_id]
                                .entry(value)
                                .or_insert_with(HashSet::new)
                                .extend(ids);
                        }
                    }
                }
            }));
        }
        
        for h in handles {
            let _ = h.join();
        }
    }

    fn update_index(&self, record: &Record) -> std::io::Result<()> {
        let mut idx_guard = self.indexes.write();
        let shard_id = record.id.as_bytes()[0] as usize % self.shard_count;
        
        for (key, value) in [
            ("name", record.name.clone()),
            ("age", record.age.to_string()),
            ("email", record.email.clone()),
        ] {
            let index_maps = idx_guard
                .entry(key.to_string())
                .or_insert_with(|| vec![HashMap::new(); self.shard_count]);
                
            index_maps[shard_id]
                .entry(value)
                .or_insert_with(HashSet::new)
                .insert(record.id.to_string());
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn create(&self, mut record: Record) {
        if record.id.is_empty() {
            record.id = Uuid::new_v4().to_string(); 
        }
        let mut data_guard = self.data.write();
        data_guard.insert(record.id.to_string(), record.clone());
        let _ = self.update_index(&record);
    }

    // Add parallel batch operations
    pub fn batch_create(&self, records: Vec<Record>) {
        let chunks: Vec<_> = records.chunks(self.batch_size.get()).collect();
        
        // Process chunks in parallel and collect results
        let all_results: Vec<_> = chunks.par_iter().map(|chunk| {
            let mut local_results = Vec::with_capacity(chunk.len());
            let mut local_data = HashMap::with_capacity(chunk.len());
            let mut local_indexes = HashMap::new();
            
            for record in chunk.iter() {
                let record_id = if record.id.is_empty() {
                    Uuid::new_v4().to_string()
                } else {
                    record.id.clone()
                };
                
                let record_with_id = Record {
                    id: record_id.clone(),
                    ..record.clone()
                };
                
                local_results.push(record_with_id.clone());
                local_data.insert(record_id.clone(), record_with_id.clone());

                // Prepare index updates locally
                let name_shard = record_with_id.name.as_bytes()[0] as usize % INDEX_SHARD_SIZE;
                let age_shard = record_with_id.age as usize % INDEX_SHARD_SIZE;
                let email_shard = record_with_id.email.as_bytes()[0] as usize % INDEX_SHARD_SIZE;

                for (key, value, shard) in [
                    ("name", record_with_id.name.clone(), name_shard),
                    ("age", record_with_id.age.to_string(), age_shard),
                    ("email", record_with_id.email.clone(), email_shard),
                ] {
                    local_indexes
                        .entry(format!("{}_{}", key, shard))
                        .or_insert_with(Vec::new)
                        .push((value, record_id.clone()));
                }
            }
            (local_data, local_indexes)
        }).collect();

        // Update main data storage
        let mut data_guard = self.data.write();
        let mut idx_guard = self.indexes.write();

        // Merge all results
        for (local_data, local_indexes) in all_results {
            // Update main data
            data_guard.extend(local_data);

            // Update indexes
            for (key, entries) in local_indexes {
                let index_shard = idx_guard
                    .entry(key.clone())
                    .or_insert_with(Vec::new);

                for (value, id) in entries {
                    let shard_id = id.as_bytes()[0] as usize % self.shard_count;
                    if index_shard.len() <= shard_id {
                        index_shard.resize(shard_id + 1, HashMap::new());
                    }
                    index_shard[shard_id]
                        .entry(value)
                        .or_insert_with(HashSet::new)
                        .insert(id);
                }
            }
        }
    }

    // Add batch update method
    pub fn batch_update(&self, updates: Vec<(String, Record)>) -> usize {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let successful = Arc::new(AtomicUsize::new(0));
        let chunks: Vec<_> = updates.chunks(self.batch_size.get()).collect();
        
        chunks.par_iter().for_each(|chunk| {
            let mut local_updates = Vec::with_capacity(chunk.len());
            for (id, record) in chunk.iter() {
                if let Some(updated) = self.update(id, record) {
                    local_updates.push(updated);
                }
            }
            successful.fetch_add(local_updates.len(), Ordering::Relaxed);
        });
        
        successful.load(Ordering::Relaxed)
    }

    // Add batch delete method
    pub fn batch_delete(&self, ids: Vec<String>) -> usize {
        let successful = Arc::new(AtomicUsize::new(0));
        let chunks: Vec<_> = ids.chunks(self.batch_size.get()).collect();
        
        chunks.par_iter().for_each(|chunk| {
            for id in *chunk {
                if self.delete(id) {
                    successful.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        
        successful.load(Ordering::Relaxed)
    }

    pub fn read_all(&self) -> Vec<Record> {
        let data_guard = self.data.read();
        data_guard.values().cloned().collect()
    }

    pub fn update(&self, id: &str, updates: &Record) -> Option<Record> {
        let mut result = None;
        let mut data_guard = self.data.write();
        if let Some(existing) = data_guard.get_mut(id) {
            existing.name = updates.name.clone();
            existing.age = updates.age;
            existing.email = updates.email.clone();
            result = Some(existing.clone());
        }
        if let Some(updated) = &result {
            let _ = self.update_index(updated);
        }
        result
    }

    // Update find_by_key to search across all shards
    pub fn find_by_key(&self, key: &str, value: &str) -> Vec<Record> {
        let idx_guard = self.indexes.read();
        let data_guard = self.data.read();
        let results = Arc::new(Mutex::new(Vec::new()));
        
        // Search across all possible shards
        (0..INDEX_SHARD_SIZE).into_par_iter().for_each(|shard| {
            let shard_key = format!("{}_{}", key, shard);
            if let Some(index) = idx_guard.get(&shard_key) {
                for shard in index {
                    if let Some(ids) = shard.get(value) {
                        let shard_results: Vec<Record> = ids.par_iter()
                            .filter_map(|id| data_guard.get(id).cloned())
                            .collect();
                        let mut results_guard = results.lock().unwrap();
                        results_guard.extend(shard_results);
                    }
                }
            }
        });
        Arc::try_unwrap(results).unwrap().into_inner().unwrap()
    }

    // Add paginated query method
    pub fn find_by_key_paginated(&self, key: &str, value: &str, page: usize, per_page: usize) -> (Vec<Record>, usize) {
        let cache_key = format!("{}={}:{}:{}", key, value, page, per_page);
        
        // Check cache first
        {
            let cache = self.query_cache.read();
            if let Some((results, timestamp)) = cache.results.get(&cache_key) {
                if timestamp.elapsed() < cache.ttl {
                    cache.hits.fetch_add(1, Ordering::Relaxed);
                    return (results.clone(), results.len());
                }
            }
            cache.misses.fetch_add(1, Ordering::Relaxed);
        }

        // Perform query
        let results = self.find_by_key(key, value);
        let total = results.len();
        let start = page * per_page;
        let end = (start + per_page).min(total);
        let page_results = results[start..end].to_vec();

        // Update cache
        {
            let mut cache = self.query_cache.write();
            if cache.results.len() >= cache.max_size {
                // Remove oldest entry
                let oldest_key = cache.results.keys()
                    .min_by_key(|k| cache.results.get(*k).map(|(_, ts)| ts.elapsed()).unwrap_or(Duration::ZERO))
                    .cloned();
                if let Some(oldest) = oldest_key {
                    cache.results.remove(&oldest);
                }
            }
            cache.results.insert(cache_key, (page_results.clone(), Instant::now()));
        }

        // Update index statistics
        {
            let mut stats = self.index_stats.write();
            let field_stats = stats.entry(key.to_string()).or_insert_with(HashMap::new);
            *field_stats.entry(value.to_string()).or_insert(0) += 1;
        }

        (page_results, total)
    }

    // Add method to get index statistics
    pub fn get_index_stats(&self) -> HashMap<String, Vec<(String, usize)>> {
        let stats = self.index_stats.read();
        let mut formatted = HashMap::new();
        
        for (field, values) in stats.iter() {
            let mut stats_vec: Vec<_> = values.iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            stats_vec.sort_by(|a, b| b.1.cmp(&a.1));
            formatted.insert(field.clone(), stats_vec);
        }
        
        formatted
    }

    pub fn analyze_data_distribution(&self) -> HashMap<String, HashMap<String, f64>> {
        let mut distribution = HashMap::new();
        let data_guard = self.data.read();
        let total_records = data_guard.len() as f64;
        if total_records == 0.0 { return distribution; }

        let mut age_dist = HashMap::new();
        let mut name_dist = HashMap::new();
        let mut email_dist = HashMap::new();

        for record in data_guard.values() {
            *age_dist.entry(format!("{}-{}", (record.age/10)*10, (record.age/10)*10+9))
                .or_insert(0.0) += 1.0;
            if let Some(c) = record.name.chars().next() {
                *name_dist.entry(c.to_string()).or_insert(0.0) += 1.0;
            }
            if let Some(domain) = record.email.split('@').nth(1) {
                *email_dist.entry(domain.to_string()).or_insert(0.0) += 1.0;
            }
        }

        for dist in [&mut age_dist, &mut name_dist, &mut email_dist] {
            for count in dist.values_mut() {
                *count = (*count / total_records) * 100.0;
            }
        }

        distribution.insert("age_groups".to_string(), age_dist);
        distribution.insert("name_first_letters".to_string(), name_dist);
        distribution.insert("email_domains".to_string(), email_dist);
        distribution
    }

    pub fn print_data_distribution(&self) {
        let distribution = self.analyze_data_distribution();
        println!("\n=== Data Distribution Analysis ===");
        
        for (category, dist) in [
            ("Age Groups", "age_groups"),
            ("Name First Letters", "name_first_letters"),
            ("Email Domains", "email_domains"),
        ] {
            if let Some(data) = distribution.get(dist) {
                println!("\n{}:", category);
                let mut items: Vec<_> = data.iter().collect();
                items.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
                for (key, pct) in items.iter().take(5) {
                    println!("  {}: {:.1}%", key, pct);
                }
            }
        }
        println!();
    }

    pub fn delete(&self, id: &str) -> bool {
        let mut data_guard = self.data.write();
        if let Some(old_record) = data_guard.remove(id) {
            let mut idx_guard = self.indexes.write();
            let shard_id = id.as_bytes()[0] as usize % self.shard_count;
            
            for (key, value) in [
                ("name", old_record.name),
                ("age", old_record.age.to_string()),
                ("email", old_record.email),
            ] {
                if let Some(shards) = idx_guard.get_mut(key) {
                    if let Some(shard) = shards.get_mut(shard_id) {
                        if let Some(ids) = shard.get_mut(&value) {
                            ids.remove(id);
                        }
                    }
                }
            }
            true
        } else {
            false
        }
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

    pub async fn run_benchmark(&self) -> std::io::Result<()> {
        let entries = 1_000_000;
        let mut records = Vec::with_capacity(entries);
        for i in 0..entries {
            records.push(Record {
                id: String::new(), 
                name: format!("User{}", i),
                age: (i % 100) as u32,
                email: format!("user{}@example.com", i),
            });
        }
        let create_start = Instant::now();
        self.batch_create(records);
        println!("Create {} entries: {:?}", entries, create_start.elapsed());

        let read_start = Instant::now();
        let data = self.read_all();
        println!("Read {} entries: {:?}", data.len(), read_start.elapsed());

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

        let delete_start = Instant::now();
        for item in data.iter().take(2000) {
            self.delete(&item.id);
        }
        println!("Batch delete 2000 entries: {:?}", delete_start.elapsed());

        let save_start = Instant::now();
        self.save().await?;
        println!("Save to disk: {:?}", save_start.elapsed());
        
        println!("=== Benchmark Complete ===");
        
        // Add total entries count
        let final_count = self.data.read().len();
        println!("Total entries in database: {}", final_count);
        Ok(())
    }

    async fn save_chunk(chunk: Vec<Record>, path: String, chunk_id: usize) -> std::io::Result<()> {
        let chunk_path = format!("{}.{}", path, chunk_id);
        
        // Serialize the chunk
        let serialized = rmp_serde::to_vec(&chunk)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        
        // Compress the serialized data
        let mut compressed = Vec::with_capacity(serialized.len() + 1024);
        compressed.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
        compressed.extend(lz4_flex::compress(&serialized));
        
        // Write to file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&chunk_path)?;

        // Write in smaller chunks to avoid memory spikes
        for buffer in compressed.chunks(8 * 1024 * 1024) {
            file.write_all(buffer)?;
        }
        file.sync_all()?;

        println!("Saved chunk {} with {} records ({} bytes)", 
            chunk_id, chunk.len(), compressed.len());
        Ok(())
    }
    pub async fn run_live_benchmark(&self) -> std::io::Result<()> {
        let mut total_processed = 0;
        let start_time = Instant::now();
        let help_text = "\nAvailable commands:\n\
            add <count>              - Add records (default: 1)\n\
            read                     - Read all records\n\
            update <count>           - Update records (default: 10)\n\
            delete <count>           - Delete records (default: 10)\n\
            query <field>=<value>    - Query records\n\
            random <count>           - Run random queries (default: 10)\n\
            benchmark                - Run full benchmark\n\
            save                     - Save database\n\
            stats                    - Show statistics\n\
            analyze                  - Show data distribution\n\
            help                     - Show this help\n\
            exit                     - Exit program\n";

        println!("\n=== PookieDB Live Benchmark ===");
        println!("{}", help_text);
        println!("=====================================\n");

        let stdin = io::stdin();
        let mut handle = stdin.lock();
        let mut input = String::new();
        loop {
            print!("> ");
            io::stdout().flush()?;
            input.clear();
            handle.read_line(&mut input)?;
            let parts: Vec<&str> = input.trim().split_whitespace().collect();
            if parts.is_empty() { continue; }
            match parts[0] {
                "help" => println!("{}", help_text),
                "add" | "1" => {
                    let count = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
                    let mut records = Vec::with_capacity(count);
                    for i in 0..count {
                        records.push(Record {
                            id: String::new(),
                            name: format!("User{}", total_processed + i),
                            age: ((total_processed + i) % 100) as u32,
                            email: format!("user{}@example.com", total_processed + i),
                        });
                    }
                    let start = Instant::now();
                    self.batch_create(records);
                    total_processed += count;
                    println!("Added {} records in {:?}", count, start.elapsed());
                }
                "read" | "2" => {
                    let start = Instant::now();
                    let records = self.read_all();
                    println!("Read {} records in {:?}", records.len(), start.elapsed());
                }
                "update" | "3" => {
                    let count = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10);
                    let records = self.read_all();
                    let start = Instant::now();
                    for record in records.iter().take(count) {
                        let mut updated = record.clone();
                        updated.age += 1;
                        self.update(&record.id, &updated);
                    }
                    println!("Updated {} records in {:?}", count, start.elapsed());
                }
                "delete" | "4" => {
                    let count = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10);
                    let records = self.read_all();
                    let start = Instant::now();
                    for record in records.iter().take(count) {
                        self.delete(&record.id);
                    }
                    println!("Deleted {} records in {:?}", count, start.elapsed());
                }
                "query" | "5" => {
                    if let Some(query_str) = parts.get(1) {
                        if let Some((field, value)) = query_str.split_once('=') {
                            let start = Instant::now();
                            let results = self.find_by_key(field, value);
                            println!(
                                "\nQuery {}={} returned {} results in {:?}",
                                field, value, results.len(), start.elapsed()
                            );
                            println!("\nResults:");
                            for (i, record) in results.iter().enumerate() {
                                println!("{}. {}: {} (age: {}, email: {})",
                                    i + 1,
                                    record.id,
                                    record.name,
                                    record.age,
                                    record.email
                                );
                                if i >= 9 && results.len() > 10 {
                                    println!("... and {} more records", results.len() - 10);
                                    break;
                                }
                            }
                            println!(); // Extra newline for readability
                        } else {
                            println!("Invalid query format. Use: query field=value (e.g., query age=25)");
                        }
                    } else {
                        println!("Missing query parameters. Use: query field=value (e.g., query age=25)");
                    }
                }
                "random" => {
                    let count = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10);
                    let start = Instant::now();
                    self.run_random_queries(count);
                    println!("Ran {} random queries in {:?}", count, start.elapsed());
                }
                "benchmark" | "6" => {
                    self.run_benchmark().await?;
                }
                "save" | "7" => {
                    let start = Instant::now();
                    self.save().await?;
                    println!("Database saved in {:?}", start.elapsed());
                }
                "stats" | "8" => {
                    let elapsed = start_time.elapsed();
                    let count = self.data.read().len();
                    println!("\n=== Database Statistics ===");
                    println!("Total records: {}", count);
                    println!("Uptime: {:?}", elapsed);
                    println!("Average insert rate: {:.2} records/sec", 
                        total_processed as f64 / elapsed.as_secs_f64());
                }
                "batch-update" => {
                    let count = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10);
                    let records = self.read_all();
                    let updates: Vec<_> = records.iter()
                        .take(count)
                        .map(|r| (r.id.clone(), Record { age: r.age + 1, ..r.clone() }))
                        .collect();
                    
                    let start = Instant::now();
                    let successful = self.batch_update(updates);
                    println!("Batch updated {} records in {:?}", successful, start.elapsed());
                }
                "batch-delete" => {
                    let count = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10);
                    let ids: Vec<_> = self.read_all().iter()
                        .take(count)
                        .map(|r| r.id.clone())
                        .collect();
                    
                    let start = Instant::now();
                    let successful = self.batch_delete(ids);
                    println!("Batch deleted {} records in {:?}", successful, start.elapsed());
                }
                "query-page" => {
                    if let (Some(query_str), Some(page), Some(per_page)) = (parts.get(1), parts.get(2), parts.get(3)) {
                        if let Some((field, value)) = query_str.split_once('=') {
                            let page: usize = page.parse().unwrap_or(0);
                            let per_page: usize = per_page.parse().unwrap_or(10);
                            
                            let start = Instant::now();
                            let (results, total) = self.find_by_key_paginated(field, value, page, per_page);
                            println!(
                                "\nQuery {}={} returned {} results (page {} of {}, {} per page) in {:?}",
                                field, value, total, 
                                page + 1, (total + per_page - 1) / per_page,
                                per_page, start.elapsed()
                            );
                            
                            println!("\nResults:");
                            for (i, record) in results.iter().enumerate() {
                                println!("{}. {}: {} (age: {}, email: {})",
                                    i + 1 + (page * per_page),
                                    record.id,
                                    record.name,
                                    record.age,
                                    record.email
                                );
                            }
                            println!();
                        }
                    }
                }
                "cache-stats" => {
                    let cache = self.query_cache.read();
                    println!("\nCache Statistics:");
                    println!("Cache size: {}/{}", cache.results.len(), cache.max_size);
                    println!("Hits: {}", cache.hits.load(Ordering::Relaxed));
                    println!("Misses: {}", cache.misses.load(Ordering::Relaxed));
                    println!();
                }
                "index-stats" => {
                    let stats = self.get_index_stats();
                    println!("\nIndex Usage Statistics:");
                    for (field, values) in stats {
                        println!("\n{}:", field);
                        for (value, count) in values.iter().take(5) {
                            println!("  {}: {} queries", value, count);
                        }
                    }
                    println!();
                }
                "analyze" => {
                    let start = Instant::now();
                    self.print_data_distribution();
                    println!("Analysis completed in {:?}", start.elapsed());
                }
                "exit" | "9" | "quit" => break,
                _ => println!("Unknown command. Type 'help' to see available commands."),
            }
        }

        println!("Saving final state...");
        self.save().await?;
        
        let final_count = self.data.read().len();
        println!("Final record count: {}", final_count);
        Ok(())
    }
    pub async fn run() -> std::io::Result<()> {
        let db = Database::new("rust_benchmark_data.pookie");
        println!("Initializing database at {}", db.path);
        
        match db.init().await {
            Ok(_) => {
                println!("Database initialized successfully");
                if let Err(e) = db.run_live_benchmark().await {
                    eprintln!("Benchmark failed: {}", e);
                }
            },
            Err(e) => eprintln!("Failed to init database: {} ({})", e, e.kind()),
        }
        Ok(())
    }
}
#[tokio::main]
async fn main() {
    if let Err(e) = Database::run().await {
        eprintln!("Application error: {}", e);
    }
} // 1069 lines 😼