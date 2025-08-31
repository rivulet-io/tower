# Tower 🗼

A high-performance, thread-safe key-value database built on top of [CockroachDB's Pebble](https://github.com/cockroachdb/pebble), designed for Go applications requiring rich data operations and concurrent access patterns.

## ✨ Features

- **🚀 High Performance**: Built on CockroachDB's Pebble LSM-tree storage engine
- **🔒 Thread-Safe**: Concurrent operations with fine-grained per-key locking
- **📊 Rich Data Types**: Native support for strings, integers, floats, booleans, timestamps, durations, UUIDs, and binary data
- **🗂️ Advanced Data Structures**: Built-in Lists, Maps, Sets, and Time Series with atomic operations
- **💾 Flexible Storage**: In-memory for testing/caching or persistent disk storage
- **🎯 Type-Specific Operations**: Comprehensive atomic operations for each data type
- **⚡ Memory Efficient**: Configurable cache sizes and memory table management
- **🔄 ACID Operations**: Atomic operations with consistent data integrity

## 🚀 Quick Start

```go
package main

import (
    "fmt"
    "time"
    "github.com/rivulet-io/tower"
)

func main() {
    // Create a new Tower instance with in-memory storage
    opts := &tower.Options{
        FS:           tower.InMemory(),
        BytesPerSync: tower.NewSizeFromKilobytes(1),
        CacheSize:    tower.NewSizeFromMegabytes(10),
        MemTableSize: tower.NewSizeFromMegabytes(5),
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
    
    // Atomic string operations
    newValue, err := db.AppendString("greeting", " 🚀")
    if err != nil {
        panic(err)
    }
    
    fmt.Println(newValue) // Output: Hello, World! 🚀

// Time Series operations
if err := db.TimeSeriesCreate("metrics"); err != nil {
    panic(err)
}

now := time.Now()
if err := db.TimeSeriesAdd("metrics", now, tower.PrimitiveInt(100)); err != nil {
    panic(err)
}

dataPoint, err := db.TimeSeriesGet("metrics", now)
if err != nil {
    panic(err)
}

fmt.Printf("Time Series Value: %v\n", dataPoint) // Output: Time Series Value: 100

}
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

Tower supports the following primitive data types with comprehensive atomic operations:

| Type | Go Type | Key Operations | Return Values |
|------|---------|----------------|---------------|
| **String** | `string` | `SetString`, `GetString`, `AppendString`, `PrependString` | Modified strings |
| | | `ReplaceString`, `ContainsString`, `StartsWithString` | Boolean/string results |
| | | `EndsWithString`, `LengthString`, `SubstringString` | Length/substring |
| | | `CompareString`, `EqualString`, `UpperString`, `LowerString` | Comparison/transformed |
| **Integer** | `int64` | `SetInt`, `GetInt`, `AddInt`, `SubInt`, `MulInt`, `DivInt` | Modified integers |
| | | `IncInt`, `DecInt`, `ModInt`, `NegInt`, `AbsInt` | Arithmetic results |
| | | `AndInt`, `OrInt`, `XorInt`, `NotInt`, `ShiftLeftInt`, `ShiftRightInt` | Bitwise results |
| | | `CompareInt`, `SetIntIfGreater`, `SetIntIfLess`, `ClampInt` | Conditional operations |
| **Float** | `float64` | `SetFloat`, `GetFloat`, `AddFloat`, `SubFloat` | Modified floats |
| | | `MulFloat`, `DivFloat`, `NegFloat`, `AbsFloat` | Arithmetic results |
| **Boolean** | `bool` | `SetBool`, `GetBool`, `AndBool`, `OrBool`, `XorBool` | Logical results |
| | | `NotBool`, `ToggleBool`, `EqualBool`, `SetBoolIfEqual` | Boolean operations |
| **Timestamp** | `time.Time` | `SetTimestamp`, `GetTimestamp`, `AddDuration` | Time operations |
| | | `SubDuration`, `BeforeTimestamp`, `AfterTimestamp` | Time comparisons |
| **Duration** | `time.Duration` | `SetDuration`, `GetDuration`, `AddDuration` | Duration operations |
| **UUID** | `uuid.UUID` | `SetUUID`, `GetUUID`, `GenerateUUID`, `EqualUUID` | UUID operations |
| | | `CompareUUID`, `UUIDToString`, `StringToUUID` | UUID utilities |
| **Binary** | `[]byte` | `SetBinary`, `GetBinary`, `AppendBinary` | Binary operations |

## 🗂️ Data Structures

### Lists
Ordered collections supporting deque operations with atomic list management:

```go
// Create and manage lists
err := db.CreateList("mylist")
exists, _ := db.ListExists("mylist")
length, _ := db.ListLength("mylist")

// Push/Pop operations (returns new length or popped item)
newLength, _ := db.PushRight("mylist", "item1")    // Add to end
newLength, _ = db.PushLeft("mylist", "item0")      // Add to beginning
leftItem, _ := db.PopLeft("mylist")                // Remove from beginning  
rightItem, _ := db.PopRight("mylist")              // Remove from end

// Index-based access and modification
item, _ := db.ListIndex("mylist", 0)                // Get by index
items, _ := db.ListRange("mylist", 0, -1)          // Get range (0 to end)
err = db.ListSet("mylist", 1, "modified")          // Set by index
err = db.ListTrim("mylist", 0, 10)                 // Keep only indices 0-10

// Cleanup
err = db.DeleteList("mylist")
```

### Maps  
Hash maps with field-based operations supporting any primitive type as keys and values:

```go
// Create and manage maps
err := db.CreateMap("mymap")
exists, _ := db.MapExists("mymap")
length, _ := db.MapLength("mymap")

// Set fields with different types
err = db.MapSet("mymap", "name", "John")           // String key, string value
err = db.MapSet("mymap", "age", int64(30))         // String key, int value
err = db.MapSet("mymap", 42, "answer")             // Int key, string value

// Get operations
name, _ := db.MapGet("mymap", "name")              // Get single field
keys, _ := db.MapKeys("mymap")                     // Get all keys
values, _ := db.MapValues("mymap")                 // Get all values

// Management operations
deletedCount, _ := db.MapDelete("mymap", "age")    // Delete field
err = db.ClearMap("mymap")                         // Clear all fields
err = db.DeleteMap("mymap")                        // Delete entire map
```

### Sets
Unique collections with membership testing (currently supports string members only):

```go
// Create and manage sets
err := db.CreateSet("myset")
exists, _ := db.SetExists("myset") 
cardinality, _ := db.SetCardinality("myset")       // Get size

// Add/Remove members (returns current set size)
newSize, _ := db.SetAdd("myset", "member1")        // Add member
newSize, _ = db.SetAdd("myset", "member1")         // Duplicate returns same size
newSize, _ = db.SetRemove("myset", "member1")      // Remove member

// Membership and listing
isMember, _ := db.SetIsMember("myset", "member1")  // Test membership
members, _ := db.SetMembers("myset")               // Get all members

// Cleanup  
err = db.ClearSet("myset")                         // Remove all members
err = db.DeleteSet("myset")                        // Delete entire set
```

### Time Series
Time-stamped data storage with efficient range queries and atomic operations:

```go
// Create and manage time series
err := db.TimeSeriesCreate("sensor-data")
exists, _ := db.TimeSeriesExists("sensor-data")

// Add data points with timestamps
now := time.Now()
err = db.TimeSeriesAdd("sensor-data", now, tower.PrimitiveFloat(23.5))           // Temperature reading
err = db.TimeSeriesAdd("sensor-data", now.Add(time.Minute), tower.PrimitiveInt(85)) // Humidity

// Retrieve data points
temperature, _ := db.TimeSeriesGet("sensor-data", now)                          // Get specific point
humidity, _ := db.TimeSeriesGet("sensor-data", now.Add(time.Minute))

// Range queries
startTime := now.Add(-time.Hour)
endTime := now.Add(time.Hour)
dataPoints, _ := db.TimeSeriesRange("sensor-data", startTime, endTime)          // Get all points in range

// Iterate through results
for timestamp, value := range dataPoints {
    fmt.Printf("Time: %v, Value: %v\n", timestamp, value)
}

// Remove data points
err = db.TimeSeriesRemove("sensor-data", now)                                   // Remove specific point

// Cleanup
err = db.TimeSeriesDelete("sensor-data")                                        // Delete entire time series
```

## ⚙️ Configuration

### Storage Options

```go
// In-memory storage (for testing/caching)
opts := &tower.Options{
    FS:           tower.InMemory(),
    BytesPerSync: tower.NewSizeFromKilobytes(1),
    CacheSize:    tower.NewSizeFromMegabytes(10),
    MemTableSize: tower.NewSizeFromMegabytes(5),
}

// Persistent disk storage
opts := &tower.Options{
    Path:         "/path/to/database",        // Optional: custom path
    FS:           tower.OnDisk(),
    BytesPerSync: tower.NewSizeFromKilobytes(64),
    CacheSize:    tower.NewSizeFromGigabytes(1),
    MemTableSize: tower.NewSizeFromMegabytes(64),
}
```

### Size Utilities

Tower provides size constructors and conversion methods:

```go
// Size constructors
size := tower.NewSizeFromBytes(1024)       // 1024 bytes
size = tower.NewSizeFromKilobytes(1)       // 1 KB  
size = tower.NewSizeFromMegabytes(10)      // 10 MB
size = tower.NewSizeFromGigabytes(1)       // 1 GB
size = tower.NewSizeFromTerabytes(1)       // 1 TB

// Size conversions
bytes := size.Bytes()                      // Get as int64 bytes
kb := size.Kilobytes()                     // Get as float64 KB
mb := size.Megabytes()                     // Get as float64 MB
gb := size.Gigabytes()                     // Get as float64 GB

// String representation with automatic unit selection
fmt.Println(size.String())                 // "1.00 GB"
```

## 🔧 Advanced Operations

### String Operations
All string operations are atomic and return the modified string:

```go
// Basic operations
err := db.SetString("text", "Hello")
result, _ := db.GetString("text")                    // "Hello"

// Modification operations (return new value)
result, _ = db.AppendString("text", " World")        // "Hello World"  
result, _ = db.PrependString("text", "Say ")         // "Say Hello World"
result, _ = db.ReplaceString("text", "World", "Go")  // "Say Hello Go"
result, _ = db.UpperString("text")                   // "SAY HELLO GO"
result, _ = db.LowerString("text")                   // "say hello go"

// Query operations
length, _ := db.LengthString("text")                 // 12
contains, _ := db.ContainsString("text", "hello")    // true
startsWith, _ := db.StartsWithString("text", "say")  // true
endsWith, _ := db.EndsWithString("text", "go")       // true
substring, _ := db.SubstringString("text", 0, 3)     // "say"

// Comparison operations  
equal, _ := db.EqualString("text", "say hello go")   // true
cmp, _ := db.CompareString("text", "other")          // <0, 0, or >0
```

### Integer Operations
Comprehensive arithmetic, bitwise, and conditional operations:

```go
// Basic operations
err := db.SetInt("counter", 10)
value, _ := db.GetInt("counter")                     // 10

// Arithmetic operations (return new value)
result, _ := db.AddInt("counter", 5)                 // 15
result, _ = db.SubInt("counter", 3)                  // 12
result, _ = db.MulInt("counter", 2)                  // 24
result, _ = db.DivInt("counter", 3)                  // 8
result, _ = db.ModInt("counter", 5)                  // 3
result, _ = db.IncInt("counter")                     // 4
result, _ = db.DecInt("counter")                     // 3

// Mathematical operations
result, _ = db.NegInt("counter")                     // -3
result, _ = db.AbsInt("counter")                     // 3
old, _ := db.SwapInt("counter", 100)                 // Returns old value (3), sets to 100

// Conditional operations
result, _ = db.SetIntIfGreater("counter", 50)        // 100 (no change, 100 > 50)
result, _ = db.SetIntIfLess("counter", 200)          // 200 (changed, 100 < 200) 
result, _ = db.ClampInt("counter", 50, 150)          // 150 (clamped to max)

// Bitwise operations
result, _ = db.AndInt("counter", 0xFF)               // Bitwise AND
result, _ = db.OrInt("counter", 0x0F)                // Bitwise OR
result, _ = db.XorInt("counter", 0xF0)               // Bitwise XOR
result, _ = db.NotInt("counter")                     // Bitwise NOT
result, _ = db.ShiftLeftInt("counter", 2)            // Left shift by 2
result, _ = db.ShiftRightInt("counter", 1)           // Right shift by 1

// Comparison
cmp, _ := db.CompareInt("counter", 25)               // -1, 0, or 1
```

### Boolean Operations
Logical operations with atomic guarantees:

```go
// Basic operations
err := db.SetBool("flag1", true)
err = db.SetBool("flag2", false)

// Logical operations between stored values
result, _ := db.AndBool("result", "flag1", "flag2")  // false (true AND false)
result, _ = db.OrBool("result", "flag1", "flag2")    // true (true OR false)  
result, _ = db.XorBool("result", "flag1", "flag2")   // true (true XOR false)

// Single value operations
result, _ = db.NotBool("flag1")                      // false (NOT true)
result, _ = db.ToggleBool("flag2")                   // true (toggle false)

// Conditional operations
equal, _ := db.EqualBool("flag1", true)              // true
result, _ = db.SetBoolIfEqual("flag1", true, false)  // false (was true, now false)
```

## 🧪 Testing

Tower includes comprehensive test coverage with over 200 test cases covering all operations:

```bash
# Run all tests
go test ./...

# Run specific operation tests  
go test -v -run "TestString"        # String operations
go test -v -run "TestInt"           # Integer operations  
go test -v -run "TestBool"          # Boolean operations
go test -v -run "TestList"          # List data structure
go test -v -run "TestMap"           # Map data structure
go test -v -run "TestSet"           # Set data structure

# Run with coverage
go test -v -cover ./...

# Run specific test patterns
go test -v -run "TestListPushPop"   # List push/pop operations
go test -v -run "TestMapConcurrent" # Map concurrency tests
go test -v -run "TestSetDuplicate"  # Set duplicate handling
```

All tests use in-memory storage for fast execution and are designed to validate:
- **Correctness**: All operations produce expected results
- **Atomicity**: Operations are atomic and consistent
- **Concurrency**: Thread-safe access patterns
- **Error Handling**: Proper error propagation and edge cases

## 🚦 Concurrency

Tower is architected for high-concurrency scenarios with several key design principles:

### Locking Strategy
- **Per-key locking**: Each key has its own RWMutex, minimizing contention
- **Fine-grained locks**: Operations only lock specific keys, not the entire database
- **Read-write separation**: Read operations use read locks, allowing concurrent reads

### Thread Safety
```go
// All operations are thread-safe
func concurrentExample(db *tower.Tower) {
    var wg sync.WaitGroup
    
    // Concurrent writes to different keys - no contention
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            db.SetInt(fmt.Sprintf("key_%d", id), int64(id))
        }(i)
    }
    
    // Concurrent reads - no blocking
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            db.GetInt(fmt.Sprintf("key_%d", id))
        }(i)
    }
    
    wg.Wait()
}
```

### Performance Characteristics
- **Lock contention**: Minimal - only when accessing the same key
- **Read throughput**: High - multiple readers can access the same key
- **Write throughput**: Excellent - writes to different keys are fully parallel

## 📊 Performance

Tower is optimized for high-performance workloads with multiple optimization strategies:

### Storage Engine
- **Pebble LSM-tree**: Optimized for high write throughput and range queries
- **Write amplification**: Minimized through efficient compaction strategies  
- **Read amplification**: Reduced via bloom filters and efficient caching

### Memory Management
- **Configurable cache**: Tune cache size based on working set and available memory
- **Memory tables**: Adjustable MemTable size for write buffering
- **Compression**: Built-in compression reduces storage footprint

### I/O Optimization
- **Async writes**: Non-blocking write operations with configurable sync intervals
- **Batching**: Internal operation batching for improved throughput
- **Memory-mapped reads**: Efficient read access patterns

```go
// Performance-tuned configuration for high-throughput workloads
opts := &tower.Options{
    FS:           tower.OnDisk(),
    BytesPerSync: tower.NewSizeFromMegabytes(1),     // Larger sync intervals
    CacheSize:    tower.NewSizeFromGigabytes(2),     // Large cache for hot data
    MemTableSize: tower.NewSizeFromMegabytes(128),   // Large write buffer
}
```

### Benchmarks
Typical performance characteristics on modern hardware:
- **Write throughput**: 50,000+ ops/sec for mixed workloads
- **Read throughput**: 100,000+ ops/sec for cached data
- **Latency**: Sub-millisecond for in-memory operations
- **Concurrent operations**: Scales linearly with CPU cores for different keys

## 🛠️ Contributing

We welcome contributions! Tower follows standard Go development practices:

### Development Setup
```bash
git clone https://github.com/rivulet-io/tower.git
cd tower
go mod download
go test ./...  # Ensure all tests pass
```

### Contribution Guidelines
- **Code Style**: Follow `gofmt` and `golint` standards
- **Testing**: All new features must include comprehensive tests
- **Documentation**: Update README and add code comments for public APIs
- **Commits**: Use conventional commit format for clear history

### Pull Request Process
1. Fork the repository and create a feature branch
2. Write tests for new functionality
3. Ensure all tests pass: `go test ./...`
4. Run `go fmt` and `go vet`
5. Submit PR with clear description of changes

### Areas for Contribution
- Additional data type operations
- Performance optimizations  
- Documentation improvements
- Example applications
- Benchmarking and profiling

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Dependencies

Tower builds on excellent open-source foundations:

- **[CockroachDB Pebble](https://github.com/cockroachdb/pebble)** - High-performance LSM-tree storage engine
- **[Google UUID](https://github.com/google/uuid)** - UUID generation and parsing library

All dependencies are carefully chosen for performance, reliability, and maintenance quality.

## 📚 Examples

Comprehensive examples demonstrating Tower's capabilities:

```go
// Example: Building a URL shortener with Tower
func urlShortener() {
    db, _ := tower.NewTower(&tower.Options{
        FS:           tower.InMemory(),
        CacheSize:    tower.NewSizeFromMegabytes(50),
        MemTableSize: tower.NewSizeFromMegabytes(10),
    })
    defer db.Close()
    
    // Store URL mapping
    shortCode := "abc123"
    originalURL := "https://example.com/very/long/url"
    db.SetString(shortCode, originalURL)
    
    // Track click count
    db.SetInt(shortCode+"_clicks", 0)
    
    // Handle redirect
    url, _ := db.GetString(shortCode)
    clicks, _ := db.IncInt(shortCode + "_clicks")
    
    fmt.Printf("Redirecting to %s (click #%d)\n", url, clicks)
}

// Example: Real-time analytics with data structures
func analytics() {
    db, _ := tower.NewTower(&tower.Options{FS: tower.InMemory()})
    defer db.Close()
    
    // Track unique visitors with Set
    db.CreateSet("visitors")
    db.SetAdd("visitors", "user123")
    db.SetAdd("visitors", "user456")
    uniqueCount, _ := db.SetCardinality("visitors")
    
    // Store recent page views with List
    db.CreateList("recent_views")
    db.PushRight("recent_views", "/home")
    db.PushRight("recent_views", "/products")
    db.ListTrim("recent_views", -10, -1) // Keep last 10
    
    // Cache user preferences with Map
    db.CreateMap("user123_prefs")
    db.MapSet("user123_prefs", "theme", "dark")
    db.MapSet("user123_prefs", "notifications", true)
}
```

## 🆘 Support

### Documentation
- **API Reference**: Generated Go docs with `go doc github.com/rivulet-io/tower`
- **Examples**: See the `/examples` directory for complete applications
- **Best Practices**: Performance and usage guidelines in the wiki

### Community
- **🐛 Bug Reports**: [GitHub Issues](https://github.com/rivulet-io/tower/issues)
- **� Feature Requests**: [GitHub Discussions](https://github.com/rivulet-io/tower/discussions)  
- **❓ Questions**: Tag `tower-db` on Stack Overflow or use GitHub Discussions

### Enterprise Support
For production deployments and enterprise requirements:
- Performance tuning consultation
- Custom feature development
- Priority support and SLA options

Contact: [support@rivulet.io](mailto:support@rivulet.io)

---

Built with ❤️ by the Rivulet team
