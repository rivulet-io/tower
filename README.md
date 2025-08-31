# Tower 🗼

A high-performance, thread-safe key-value database built on top of [Pebble](https://github.com/cockroachdb/pebble), designed for Go applications requiring rich data operations and concurrent access patterns.

## ✨ Features

- **🚀 High Performance**: Built on CockroachDB's Pebble storage engine
- **🔒 Thread-Safe**: Concurrent operations with fine-grained locking
- **📊 Rich Data Types**: Support for strings, integers, floats, booleans, timestamps, UUIDs, and binary data
- **🗂️ Data Structures**: Built-in support for Lists, Maps, and Sets
- **💾 Flexible Storage**: In-memory or persistent disk storage
- **🎯 Type Operations**: Comprehensive operations for each data type
- **⚡ Memory Efficient**: Configurable cache and memory table sizes

## 🚀 Quick Start

```go
package main

import (
    "fmt"
    "github.com/rivulet-io/tower"
)

func main() {
    // Create a new Tower instance with in-memory storage
    opts := &tower.Options{
        FS:           tower.InMemory(),
        BytesPerSync: tower.KB(1),
        CacheSize:    tower.MB(10),
        MemTableSize: tower.MB(5),
    }
    
    db, err := tower.NewTower(opts)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    
    // Store and retrieve a string
    if err := db.SetString("greeting", "Hello, World!"); err != nil {
        panic(err)
    }
    
    value, err := db.GetString("greeting")
    if err != nil {
        panic(err)
    }
    
    fmt.Println(value) // Output: Hello, World!
}
```

## 📖 Installation

```bash
go get github.com/rivulet-io/tower
```

## 🏗️ Architecture

Tower is built with several key components:

### Core Components

- **Tower**: Main database interface with concurrent access control
- **DataFrame**: Type-safe data container supporting multiple primitive types
- **ConcurrentMap**: Thread-safe generic map implementation
- **Size**: Memory size utilities with unit conversions

### Storage Backends

- **In-Memory**: Fast, volatile storage for testing and caching
- **On-Disk**: Persistent storage with configurable sync and cache options

## 📋 Data Types

Tower supports the following primitive data types:

| Type | Go Type | Operations |
|------|---------|------------|
| String | `string` | Set, Get, Append, Prepend, Replace, Contains, Length, etc. |
| Integer | `int64` | Set, Get, Add, Sub, Mul, Div, Inc, Dec, Bitwise ops, etc. |
| Float | `float64` | Set, Get, Add, Sub, Mul, Div, etc. |
| Boolean | `bool` | Set, Get, And, Or, Xor, Not, Toggle, etc. |
| Timestamp | `int64` | Unix timestamp operations |
| Duration | `time.Duration` | Time duration operations |
| UUID | `uuid.UUID` | UUID operations |
| Binary | `[]byte` | Binary data operations |

## 🗂️ Data Structures

### Lists
Ordered collections with push/pop operations at both ends:

```go
// Create a list
db.CreateList("mylist")

// Add elements
db.PushRight("mylist", "item1")
db.PushLeft("mylist", "item0")

// Access elements
item, _ := db.ListIndex("mylist", 0)
items, _ := db.ListRange("mylist", 0, -1)

// Modify elements
db.ListSet("mylist", 1, "modified")
db.ListTrim("mylist", 0, 10)
```

### Maps
Key-value mappings with field-based operations:

```go
// Create a map
db.CreateMap("mymap")

// Set fields
db.MapSet("mymap", "name", "John")
db.MapSet("mymap", "age", int64(30))

// Get fields
name, _ := db.MapGet("mymap", "name")
keys, _ := db.MapKeys("mymap")
values, _ := db.MapValues("mymap")
```

### Sets
Unique collections with membership testing:

```go
// Create a set
db.CreateSet("myset")

// Add members
db.SetAdd("myset", "member1")
db.SetAdd("myset", "member2")

// Test membership
exists, _ := db.SetIsMember("myset", "member1")
members, _ := db.SetMembers("myset")
```

## ⚙️ Configuration

### Storage Options

```go
// In-memory storage (for testing/caching)
opts := &tower.Options{
    FS:           tower.InMemory(),
    BytesPerSync: tower.KB(1),
    CacheSize:    tower.MB(10),
    MemTableSize: tower.MB(5),
}

// Persistent disk storage
opts := &tower.Options{
    Path:         "/path/to/database",
    FS:           tower.OnDisk(),
    BytesPerSync: tower.KB(64),
    CacheSize:    tower.GB(1),
    MemTableSize: tower.MB(64),
}
```

### Size Utilities

Tower provides convenient size constructors:

```go
tower.Bytes(1024)    // 1024 bytes
tower.KB(1)          // 1 KB
tower.MB(10)         // 10 MB
tower.GB(1)          // 1 GB
tower.TB(1)          // 1 TB
```

## 🔧 Advanced Operations

### String Operations
```go
db.SetString("text", "Hello")
db.AppendString("text", " World")    // "Hello World"
db.PrependString("text", "Say ")     // "Say Hello World"
db.ReplaceString("text", "World", "Go") // "Say Hello Go"
length, _ := db.LengthString("text") // 12
```

### Integer Operations
```go
db.SetInt("counter", 10)
db.AddInt("counter", 5)              // 15
db.MulInt("counter", 2)              // 30
db.IncInt("counter")                 // 31
result, _ := db.CompareInt("counter", 25) // 1 (greater)
```

### Boolean Operations
```go
db.SetBool("flag1", true)
db.SetBool("flag2", false)
db.AndBool("result", "flag1", "flag2") // false
db.ToggleBool("flag2")                 // true
```

## 🧪 Testing

Tower includes comprehensive test coverage for all operations:

```bash
# Run all tests
go test ./...

# Run specific operation tests
go test -v -run "TestString"
go test -v -run "TestList"
go test -v -run "TestMap"
go test -v -run "TestSet"
```

## 🚦 Concurrency

Tower is designed for high-concurrency scenarios:

- **Fine-grained locking**: Per-key locks minimize contention
- **Thread-safe operations**: All operations are safe for concurrent use
- **Lock-free reads**: Read operations use RW locks for better performance

```go
// Safe for concurrent access
go db.SetString("key1", "value1")
go db.SetString("key2", "value2")
go db.GetString("key1")
```

## 📊 Performance

Tower is optimized for performance:

- **Pebble backend**: LSM-tree based storage for fast writes
- **Configurable caching**: Adjust cache size based on workload
- **Memory-mapped I/O**: Efficient disk access patterns
- **Batch operations**: Support for bulk operations

## 🛠️ Contributing

We welcome contributions! Please see our contributing guidelines for:

- Code style and conventions
- Testing requirements
- Pull request process
- Issue reporting

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Dependencies

- [Pebble](https://github.com/cockroachdb/pebble) - High-performance storage engine
- [UUID](https://github.com/google/uuid) - UUID generation and parsing

## 📚 Examples

Check out the [examples](examples/) directory for more detailed usage examples:

- Basic CRUD operations
- Data structure usage
- Concurrent access patterns
- Performance benchmarks

## 🆘 Support

- 📖 Documentation: [Wiki](https://github.com/rivulet-io/tower/wiki)
- 🐛 Issues: [GitHub Issues](https://github.com/rivulet-io/tower/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/rivulet-io/tower/discussions)

---

Built with ❤️ by the Rivulet team
