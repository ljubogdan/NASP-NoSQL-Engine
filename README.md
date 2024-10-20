### NASP-NoSQL-Engine

## Struktura projekta (generisano - treba ažurirati pri dodavanju)

NASP-NoSQL-Engine/
│
├── cmd/
│   └── NASP-NoSQL-Engine/
│       └── main.go # Ulazna tačka aplikacije
│
├── internal/
│   ├── wal/
│   │   ├── wal.go
│   │   └── wal_test.go
│   │
│   ├── memtable/
│   │   ├── memtable.go
│   │   └── memtable_test.go
│   │
│   ├── sstable/
│   │   ├── sstable.go
│   │   └── sstable_test.go
│   │
│   ├── cache/
│   │   ├── lru_cache.go
│   │   └── lru_cache_test.go
│   │
│   ├── block_manager/
│   │   ├── block_manager.go 
│   │   └── block_manager_test.go
│   │
│   └── compaction/
│       ├── compaction.go
│       └── compaction_test.go
│
├── pkg/
│   └── config/
│       ├── config.go
│       └── config_test.go
│
├── api/
│   ├── api.go
│   └── api_test.go
│
└── go.mod