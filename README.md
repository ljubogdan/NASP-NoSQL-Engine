# Key-Value Engine

A simple Key-Value store engine implemented as a console application for the course **Advanced Algorithms and Data Structures**.

---

## Table of Contents

- [Overview](#overview)  
- [Features](#features)  
- [Architecture](#architecture)  
  - [Write-Ahead Log (WAL)](#write-ahead-log-wal)  
  - [Memtable](#memtable)  
  - [SSTable](#sstable)  
  - [Cache](#cache)  
  - [Block Manager](#block-manager)  
- [Additional Features](#additional-features)  
- [Usage](#usage)  
- [Configuration](#configuration)  
- [Authors](#authors)  

---

## Overview

This project implements a basic Key-Value storage engine designed to handle efficient read and write operations using modern data structures and algorithms. It supports persistence through SSTables and guarantees data durability with a Write-Ahead Log.

---

## Features

- Basic operations: `PUT(key, value)`, `GET(key)`, `DELETE(key)`
- Write path: logging with WAL, in-memory storage with Memtable, persistent SSTables
- Read path: searching in Memtable, Cache, and SSTables sequentially
- Data integrity checks using CRC and Merkle trees
- Cache with Least Recently Used (LRU) eviction strategy
- Support for probabilistic data structures (Bloom Filter, Count-Min Sketch, etc.)
- Rate limiting using Token Bucket algorithm

---

## Architecture

### Write-Ahead Log (WAL)

- Durable append-only log to store write operations before applying them
- Segmented log files with CRC checks for data integrity
- Supports fragmentation and padding to align with block sizes

### Memtable

- In-memory data structure for fast reads and writes
- Implemented using efficient structures like HashMap, Skip list, or B-tree
- Supports read-only and read-write instances for concurrency control

### SSTable

- Immutable sorted files on disk storing key-value pairs
- Includes multiple components:
  - Data blocks
  - Bloom Filter for fast membership checks
  - Index and Summary for efficient lookups
  - Metadata with Merkle tree hashes for integrity verification

### Cache

- In-memory cache to speed up reads
- Uses LRU eviction to manage limited space

### Block Manager

- Manages disk blocks of sizes 4KB, 8KB, or 16KB
- Handles caching and alignment for optimal I/O

---

## Additional Features

- External configuration files (JSON or YAML)
- Rate limiting with Token Bucket algorithm to prevent overload
- Support for probabilistic data structures like:
  - Bloom Filter
  - Count-Min Sketch
  - HyperLogLog
  - SimHash
- Scan operations:
  - Prefix scans (`PREFIX_SCAN`)
  - Range scans (`RANGE_SCAN`)

---

## Usage

todo later!

## Configuration
Configuration is loaded from a config.json file if present. 
Default settings will be used if no configuration file is found. 
This includes parameters such as block size, cache size, and WAL segment size.

## Authors
Bogdan Ljubinković, Miljan Jokić, Dragan Gavrić

Faculty of Technical Sciences, University of Novi Sad 2025
