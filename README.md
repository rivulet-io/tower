# Tower 🗼

A high-performance, distributed key-value database for Go, built on [CockroachDB's Pebble](https://github.com/cockroachdb/pebble). Tower can operate as a powerful standalone engine or be deployed as a scalable, distributed cluster using an integrated NATS-based mesh network.

## ✨ Features

- **🚀 High Performance**: Built on CockroachDB's Pebble LSM-tree storage engine.
- **🌐 Distributed Clustering**: Scalable and resilient clustering powered by NATS for high availability and load distribution (`mesh` package).
    - **Cluster Nodes**: Full-fledged members participating in routing and data distribution.
    - **Gateway Connections**: Connect multiple independent Tower clusters into a super-cluster.
    - **Leaf Nodes**: Lightweight edge nodes to extend the network without complex routing overhead.
- **🔒 Thread-Safe**: Concurrent operations with fine-grained per-key locking.
- **📊 Rich Data Types**: Native support for strings, integers, floats, booleans, timestamps, durations, UUIDs, binary data, BigInts, and Decimals.
- **🗂️ Advanced Data Structures**: Built-in Lists, Maps, Sets, Time Series, and Bloom Filters with atomic operations.
- **🔐 Secure Password Storage**: Multiple hashing algorithms (Argon2id, Bcrypt, Scrypt, PBKDF2) with configurable parameters.
- **⏰ TTL Support**: Automatic expiration of keys with configurable time-to-live.
- **💾 Flexible Storage**: In-memory for testing/caching or persistent disk storage.
- **🎯 Type-Specific Operations**: Comprehensive atomic operations for each data type provided by the `op.Operator`.

## 🚀 Quick Start

### Standalone Engine (`op.Operator`)

Use the `op.Operator` for a high-performance, single-node database directly in your Go application.

```go
package main

import (
    "fmt"
    "log"

    "github.com/rivulet-io/tower/op"
    "github.com/rivulet-io/tower/util/size"
)

func main() {
    // Create a new Operator instance with in-memory storage
    opts := &op.Options{
        FS:           op.InMemory(),
        BytesPerSync: size.NewSizeFromKilobytes(1),
        CacheSize:    size.NewSizeFromMegabytes(10),
        MemTableSize: size.NewSizeFromMegabytes(5),
    }
    
    db, err := op.NewOperator(opts)
    if err != nil {
        log.Fatalf("Failed to create operator: %v", err)
    }
    defer db.Close()
    
    // Store and retrieve a string
    if err := db.SetString("greeting", "Hello, Operator!"); err != nil {
        panic(err)
    }
    
    value, err := db.GetString("greeting")
    if err != nil {
        panic(err)
    }
    
    fmt.Println(value) // Output: Hello, Operator!
}
```

### Distributed Cluster (`mesh` package)

Create a distributed network of Tower nodes using the `mesh` package, which leverages NATS for clustering, routing, and high availability. The mesh network supports three main components: **Cluster Nodes**, **Gateways**, and **Leaf Nodes**.

**1. Starting a Cluster Node:**
A cluster node is a full member of the NATS cluster, participating in data replication and routing.

```go
// main_cluster.go
package main

import (
	"log"
	"time"

	"github.com/rivulet-io/tower/mesh"
	"github.com/rivulet-io/tower/util/size"
)

func main() {
	opts := mesh.NewClusterOptions("tower-node-1").
		WithListen("127.0.0.1", 4222).
		WithStoreDir("./node1").
		WithClusterName("tower-cluster").
		WithClusterListen("127.0.0.1", 6222).
		WithJetStreamMaxMemory(size.NewSizeFromMegabytes(256)).
		WithJetStreamMaxStore(size.NewSizeFromGigabytes(2))

	cluster, err := mesh.NewCluster(opts)
	if err != nil {
		log.Fatalf("Failed to start cluster: %v", err)
	}
	defer cluster.Close()

	log.Println("Cluster node is running...")
	select {} // Block forever
}
```

**2. Connecting Clusters with a Gateway:**
Gateways connect two or more independent clusters, allowing for large-scale, geographically distributed deployments. Core features like JetStream remain cluster-local, but basic messaging can cross gateways.

```go
// main_gateway.go
package main

import (
	"log"
	"time"

	"github.com/rivulet-io/tower/mesh"
)

func main() {
	// Define remote gateways to connect to
	remotes := mesh.NewRemoteGateways().
		Add("other-cluster", "nats://<other-cluster-gateway-ip>:7223")

	opts := mesh.NewClusterOptions("gateway-node-1").
		WithListen("127.0.0.1", 5222).
		WithClusterName("my-cluster").
		WithClusterListen("127.0.0.1", 7222).
		WithGateway("my-cluster", "127.0.0.1", 7222, "", "", remotes)

	cluster, err := mesh.NewCluster(opts)
	if err != nil {
		log.Fatalf("Failed to start gateway node: %v", err)
	}
	defer cluster.Close()

	log.Println("Gateway node is running...")
	select {}
}
```

**3. Connecting as a Leaf Node:**
A leaf node connects to a cluster to extend the network, ideal for edge devices or clients. It can access the cluster's features without participating in complex cluster routing.

```go
// main_leaf.go
package main

import (
	"log"
	"time"

	"github.comcom/rivulet-io/tower/mesh"
)

func main() {
	// Connect to one or more cluster nodes that accept leaf connections
	remotes := []string{"nats-leaf://127.0.0.1:7422"}

	opts := mesh.NewLeafOptions("leaf-1").
		WithListen("127.0.0.1", 8222).
		WithLeafRemotes(remotes)

	leaf, err := mesh.NewLeaf(opts)
	if err != nil {
		log.Fatalf("Failed to create leaf: %v", err)
	}
	defer leaf.Close()
	
	log.Println("Leaf node connected to cluster.")
	
	// The 'leaf' instance can now be used to interact with the cluster.
	// For example, using its core NATS and JetStream functionalities.
	
	select {}
}
```

## 📖 Installation

```bash
go get github.com/rivulet-io/tower
```

## 🏗️ Architecture

Tower's architecture is split into two primary packages:

- **`op` (Operator)**: The core database engine. It manages the underlying Pebble storage, provides all data-specific operations (e.g., `SetString`, `AddInt`), and ensures thread-safety through fine-grained locking. This package can be used on its own for a standalone key-value store.

- **`mesh` (Mesh Network)**: Provides the functionality to run Tower in a distributed cluster. It is built on NATS and offers a robust foundation for scaling out.
    - **`Cluster`**: A full-fledged member of a NATS cluster. It forms the backbone of the distributed system, handling data replication (via JetStream), routing, and high availability.
    - **`Gateway`**: A special mode for a `Cluster` node that connects it to other, separate clusters. This allows for building a "super-cluster" of interconnected systems, though features like JetStream remain local to each cluster.
    - **`Leaf`**: A lightweight node that connects to a `Cluster` to extend the network. It's ideal for edge computing scenarios or for clients that need to interact with the cluster's capabilities (like JetStream, KV Store) without the overhead of being a full cluster member.

This modular design allows you to start with a simple, embedded database and scale up to a complex, globally distributed system as your needs grow.

## 📋 Data Types

The `op.Operator` supports the following primitive data types with comprehensive atomic operations:

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
| **BigInt** | `*big.Int` | `SetBigInt`, `GetBigInt`, `AddBigInt`, `SubBigInt` | Large integer operations |
| | | `MulBigInt`, `DivBigInt`, `ModBigInt`, `NegBigInt` | BigInt arithmetic |
| | | `AbsBigInt`, `CmpBigInt` | BigInt utilities |
| **Decimal** | `int64,int32` | `SetDecimal`, `GetDecimal`, `AddDecimal`, `SubDecimal` | Fixed-point arithmetic |
| | | `MulDecimal`, `DivDecimal`, `CmpDecimal` | Decimal operations |
| | | `SetDecimalFromFloat`, `GetDecimalAsFloat` | Float conversion |
| **Password** | `[]byte` | `UpsertPassword`, `VerifyPassword` | Secure password hashing |
| | | Bcrypt, Scrypt, PBKDF2, Argon2i, Argon2id | Multiple algorithms |
| | | Configurable options, Salt generation | Security options |

## 🗂️ Data Structures

All data structure operations are provided by `op.Operator` and are fully thread-safe.

### Lists
Ordered collections supporting deque operations with atomic list management:

```go
// All examples assume 'db' is an initialized *op.Operator
// Create and manage lists
err := db.CreateList("mylist")
exists, _ := db.ListExists("mylist")
length, _ := db.ListLength("mylist")

// Push/Pop operations (returns new length or popped item)
newLength, _ := db.PushRight("mylist", op.PrimitiveString("item1"))    // Add to end
newLength, _ = db.PushLeft("mylist", op.PrimitiveString("item0"))      // Add to beginning
leftItem, _ := db.PopLeft("mylist")                // Remove from beginning  
rightItem, _ := db.PopRight("mylist")              // Remove from end

// Index-based access and modification
item, _ := db.ListIndex("mylist", 0)                // Get by index
items, _ := db.ListRange("mylist", 0, -1)          // Get range (0 to end)
err = db.ListSet("mylist", 1, op.PrimitiveString("modified"))          // Set by index
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
err = db.MapSet("mymap", op.PrimitiveString("name"), op.PrimitiveString("John"))           // String key, string value
err = db.MapSet("mymap", op.PrimitiveString("age"), op.PrimitiveInt(30))         // String key, int value
err = db.MapSet("mymap", op.PrimitiveInt(42), op.PrimitiveString("answer"))             // Int key, string value

// Get operations
name, _ := db.MapGet("mymap", op.PrimitiveString("name"))              // Get single field
keys, _ := db.MapKeys("mymap")                     // Get all keys
values, _ := db.MapValues("mymap")                 // Get all values

// Management operations
deletedCount, _ := db.MapDelete("mymap", op.PrimitiveString("age"))    // Delete field
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
import "time"
import "github.com/rivulet-io/tower/op"

// Create and manage time series
err := db.TimeSeriesCreate("sensor-data")
exists, _ := db.TimeSeriesExists("sensor-data")

// Add data points with timestamps
now := time.Now()
err = db.TimeSeriesAdd("sensor-data", now, op.PrimitiveFloat(23.5))           // Temperature reading
err = db.TimeSeriesAdd("sensor-data", now.Add(time.Minute), op.PrimitiveInt(85)) // Humidity

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
err = db.DeleteTimeSeries("sensor-data")                                        // Delete entire time series
```

### Bloom Filters
Probabilistic data structures for efficient membership testing with configurable false positive rates:

```go
// Create and manage bloom filters (default 3 hash slots)
err := db.CreateBloomFilter("user_cache", 0)         // Use default slots (3)
err = db.CreateBloomFilter("large_filter", 5)        // Use 5 hash slots

// Add items to the filter
err = db.BloomFilterAdd("user_cache", "user123")     // Add user ID
err = db.BloomFilterAdd("user_cache", "user456")

// Test membership (may have false positives)
exists, _ := db.BloomFilterContains("user_cache", "user123")  // true
exists, _ = db.BloomFilterContains("user_cache", "user999")   // false (or false positive)

// Get filter statistics
count, _ := db.BloomFilterCount("user_cache")        // Get item count

// Clear all items
err = db.BloomFilterClear("user_cache")              // Reset filter

// Cleanup
err = db.DeleteBloomFilter("user_cache")             // Delete entire filter
```

**Key Characteristics:**
- **Space Efficient**: Uses multiple hash functions with configurable slots (3-5)
- **Fast Operations**: Constant-time add and lookup operations
- **Probabilistic**: May return false positives but never false negatives
- **Configurable**: Adjust hash slots for different false positive rates
- **Thread-Safe**: Concurrent operations with fine-grained locking

**Use Cases:**
- **Caching**: Check cache membership before expensive lookups
- **Deduplication**: Prevent processing duplicate items
- **Security**: Rate limiting and spam detection
- **Big Data**: Large-scale data filtering and preprocessing

## ⏰ TTL Operations

Tower supports automatic key expiration through Time-To-Live (TTL) functionality, allowing keys to be automatically deleted after a specified time period:

```go
// Set TTL for a key
expireAt := time.Now().Add(1 * time.Hour)
err := db.SetTTL("session_key", expireAt)

// Remove TTL from a key
err = db.RemoveTTL("permanent_key")

// Start automatic cleanup (runs in background)
// Note: TTL timer management is not part of the core Operator and needs to be implemented at the application level.
// db.StartTTLTimer() 

// Manual cleanup of expired keys
// err = db.TruncateExpired()
```

**Key Features:**
- ✅ **Automatic Expiration**: Keys are automatically deleted when TTL expires
- ✅ **Manual Control**: Remove TTL or manually trigger cleanup
- ✅ **Thread-Safe**: All operations are atomic and concurrent-safe

**Use Cases:**
- **Session Management**: Expire user sessions automatically
- **Caching**: Set cache entries with expiration times
- **Temporary Data**: Store temporary data that should be cleaned up
- **Rate Limiting**: Implement time-based rate limiting

## 🔢 BigInt Operations

Tower provides comprehensive support for arbitrary-precision integers using Go's `math/big.Int`, perfect for cryptography, scientific computing, and applications requiring numbers larger than `int64`:

```go
import "math/big"

// Basic operations with large numbers
bigNum := new(big.Int).SetString("1234567890123456789012345678901234567890", 10)
err := db.SetBigInt("large_number", bigNum)
stored, _ := db.GetBigInt("large_number")                    // Returns *big.Int

// Arithmetic operations (all return *big.Int)
result, _ := db.AddBigInt("large_number", big.NewInt(1000000))
result, _ = db.SubBigInt("large_number", big.NewInt(500000))
result, _ = db.MulBigInt("large_number", big.NewInt(2))
result, _ = db.DivBigInt("large_number", big.NewInt(3))
result, _ = db.ModBigInt("large_number", big.NewInt(7))

// Mathematical operations
result, _ = db.NegBigInt("large_number")                    // Negation
result, _ = db.AbsBigInt("large_number")                    // Absolute value

// Comparison operations
cmp, _ := db.CmpBigInt("large_number", big.NewInt(1000))   // Returns -1, 0, or 1
fmt.Printf("Comparison result: %d\n", cmp)
```

**Key Features:**
- ✅ **Arbitrary Precision**: No overflow limits (unlike `int64`)
- ✅ **Cryptography Ready**: Perfect for RSA keys, large primes
- ✅ **Scientific Computing**: Handle astronomical numbers, physics calculations
- ✅ **Financial Applications**: Precise calculations with large monetary values
- ✅ **Thread-Safe**: All operations are atomic and concurrent-safe

## 💰 Decimal Operations

Tower implements high-precision fixed-point decimal arithmetic using `math/big.Int` for the coefficient, ideal for financial calculations, currency operations, and applications requiring exact decimal representation:

```go
// Basic decimal operations
coefficient := big.NewInt(1999)  // Represents 19.99
scale := int32(2)                // 2 decimal places
err := db.SetDecimal("price", coefficient, scale)

// Get decimal value
coeff, scale, _ := db.GetDecimal("price")
fmt.Printf("Price: %s (scale: %d)\n", coeff.String(), scale)

// Arithmetic operations with automatic scale alignment
// Add 5.50 to 19.99 = 25.49
resultCoeff, resultScale, _ := db.AddDecimal("price", big.NewInt(550), 2)
fmt.Printf("New price: %s (scale: %d)\n", resultCoeff.String(), resultScale)

// Multiply by 1.10 (10% increase) = 21.989
resultCoeff, resultScale, _ = db.MulDecimal("price", big.NewInt(110), 2)
fmt.Printf("10%% increase: %s (scale: %d)\n", resultCoeff.String(), resultScale)

// Divide by 2.0 = 9.995
resultCoeff, resultScale, _ = db.DivDecimal("price", big.NewInt(20), 1, 4) // divide by 2.0 with 4 decimal places precision
fmt.Printf("Half price: %s (scale: %d)\n", resultCoeff.String(), resultScale)

// Float conversion with Banker's rounding
err = db.SetDecimalFromFloat("rate", 0.123456789, 8)      // 8 decimal places
floatValue, _ := db.GetDecimalAsFloat("rate")
fmt.Printf("Rate as float: %.8f\n", floatValue)
```

**Key Features:**
- ✅ **Exact Precision**: No floating-point errors in financial calculations
- ✅ **Automatic Scale Alignment**: Handles different decimal places seamlessly
- ✅ **Banker's Rounding**: Statistically unbiased rounding for financial accuracy
- ✅ **Arbitrary Precision**: No overflow limits for large monetary values
- ✅ **Float Conversion**: Safe conversion to/from `float64` with precision limits
- ✅ **Thread-Safe**: All operations are atomic and concurrent-safe

**Use Cases:**
- 💳 **E-commerce**: Precise price calculations, tax computations
- 🏦 **Banking**: Interest calculations, currency conversions
- 📊 **Financial Analysis**: Portfolio valuations, risk calculations
- 🛒 **Retail**: Inventory costing, discount calculations
- 💰 **Cryptocurrency**: Precise token amounts, exchange rates

## 🔐 Password Operations

Tower provides secure password hashing and verification using industry-standard algorithms. All password operations use salt-based hashing with configurable parameters:

```go
import "github.com/rivulet-io/tower/op"

// Basic password operations
password := []byte("mySecretPassword123!")

// Store password with default options
err := db.UpsertPassword("user:123", password, op.PasswordAlgorithmArgon2id, op.DefaultPasswordSaltLength)

// Verify password
isValid, err := db.VerifyPassword("user:123", password)
if err != nil {
    // Handle error
} else if isValid {
    fmt.Println("Password is correct!")
} else {
    fmt.Println("Invalid password")
}

// Update password (overwrites existing)
newPassword := []byte("newSecurePassword456!")
err = db.UpsertPassword("user:123", newPassword, op.PasswordAlgorithmArgon2id, op.DefaultPasswordSaltLength)
```

### Supported Algorithms

Tower supports multiple cryptographic hashing algorithms:

```go
// Argon2id (recommended - most secure, modern)
err := db.UpsertPassword("user1", password, op.PasswordAlgorithmArgon2id, 16)

// Argon2i (secure, more resistant to side-channel attacks)  
err = db.UpsertPassword("user2", password, op.PasswordAlgorithmArgon2i, 16)

// Bcrypt (widely supported, good security)
err = db.UpsertPassword("user3", password, op.PasswordAlgorithmBcrypt, 16)

// Scrypt (secure, memory-hard)
err = db.UpsertPassword("user4", password, op.PasswordAlgorithmScrypt, 16)

// PBKDF2-SHA256 (compatible, but less secure than others)
err = db.UpsertPassword("user5", password, op.PasswordAlgorithmPBKDF2, 16)
```

### Custom Security Parameters

Use functional options to customize hashing parameters:

```go
// High-security Argon2id for sensitive applications
err := db.UpsertPassword("admin", password, op.PasswordAlgorithmArgon2id, 32,
    op.WithArgon2Params(
        5,        // time (iterations)
        64*1024,  // memory (64 MB)
        8,        // threads
        32,       // key length
    ))

// Custom Bcrypt cost
err = db.UpsertPassword("user", password, op.PasswordAlgorithmBcrypt, 16,
    op.WithBcryptCost(14)) // Higher cost = more secure but slower

// Custom Scrypt parameters
err = db.UpsertPassword("user", password, op.PasswordAlgorithmScrypt, 16,
    op.WithScryptParams(
        32768, // N (CPU/memory cost)
        8,     // r (block size)
        1,     // p (parallelization)
        32,    // key length
    ))

// Custom PBKDF2 parameters
err = db.UpsertPassword("user", password, op.PasswordAlgorithmPBKDF2, 16,
    op.WithPBKDF2Params(
        20000, // iterations
        32,    // key length
    ))
```

### Default Parameters

Each algorithm has secure default parameters:

| Algorithm | Default Parameters | Security Level |
|-----------|-------------------|----------------|
| **Argon2id** | time=3, memory=32MB, threads=4, keyLen=32 | ⭐⭐⭐⭐⭐ Highest |
| **Argon2i** | time=3, memory=32MB, threads=4, keyLen=32 | ⭐⭐⭐⭐⭐ Highest |
| **Bcrypt** | cost=12 | ⭐⭐⭐⭐ High |
| **Scrypt** | N=16384, r=8, p=1, keyLen=32 | ⭐⭐⭐⭐ High |
| **PBKDF2** | iterations=10000, keyLen=32 | ⭐⭐⭐ Good |

**Key Features:**
- ✅ **Multiple Algorithms**: Choose the best algorithm for your needs
- ✅ **Automatic Salt Generation**: Cryptographically secure random salts
- ✅ **Configurable Parameters**: Customize security vs. performance trade-offs
- ✅ **Stored Options**: Hashing parameters stored with password for verification
- ✅ **Thread-Safe**: All operations are atomic and concurrent-safe
- ✅ **Memory Safe**: Secure handling of sensitive password data

**Use Cases:**
- 👤 **User Authentication**: Web applications, mobile apps
- 🔒 **Access Control**: API keys, service authentication
- 🏢 **Enterprise Security**: Employee password management
- 🔐 **Secure Storage**: Configuration passwords, secrets
- 📱 **Multi-Factor**: Password component of MFA systems

## 🔒 SafeBox Operations

Tower provides secure encrypted data storage through its SafeBox feature. SafeBox allows you to store sensitive data encrypted with multiple industry-standard algorithms, ensuring data confidentiality at rest:

```go
// Basic SafeBox operations
sensitiveData := []byte("Secret API key or confidential information")
encryptionKey := []byte("your-encryption-key-here")

// Store encrypted data with AES-256-GCM
payload, err := db.UpsertSafeBox("api_key", sensitiveData, encryptionKey, op.EncryptionAlgorithmAES256GCM)
if err != nil {
    // Handle error
}

// Retrieve encrypted data (returns algorithm, encrypted data, and nonce)
algorithm, encryptedData, nonce, err := db.GetSafeBox("api_key")
if err != nil {
    // Handle error
}

// Extract and decrypt data in one operation
decryptedData, err := db.ExtractSafeBox("api_key", encryptionKey)
if err != nil {
    // Handle error - wrong key or corrupted data
}
fmt.Printf("Decrypted: %s\n", string(decryptedData))
```

### Supported Encryption Algorithms

Tower supports a comprehensive set of modern encryption algorithms:

```go
// AES variants with GCM mode
db.UpsertSafeBox("data1", data, key, op.EncryptionAlgorithmAES128GCM)   // AES-128-GCM
db.UpsertSafeBox("data2", data, key, op.EncryptionAlgorithmAES192GCM)    // AES-192-GCM  
db.UpsertSafeBox("data3", data, key, op.EncryptionAlgorithmAES256GCM)    // AES-256-GCM (recommended)

// ChaCha20-Poly1305 variants (excellent for mobile/ARM)
db.UpsertSafeBox("data4", data, key, op.EncryptionAlgorithmChaCha20Poly1305)   // ChaCha20-Poly1305
db.UpsertSafeBox("data5", data, key, op.EncryptionAlgorithmXChaCha20Poly1305)  // XChaCha20-Poly1305

// ASCON variants (lightweight, IoT-optimized)
db.UpsertSafeBox("data6", data, key, op.EncryptionAlgorithmAscon128)     // ASCON-128
db.UpsertSafeBox("data7", data, key, op.EncryptionAlgorithmAscon128a)    // ASCON-128a
db.UpsertSafeBox("data8", data, key, op.EncryptionAlgorithmAscon80pq)    // ASCON-80pq (post-quantum)

// Camellia variants (ISO standard, government-approved)
db.UpsertSafeBox("data9", data, key, op.EncryptionAlgorithmCamellia128GCM)  // Camellia-128-GCM
db.UpsertSafeBox("data10", data, key, op.EncryptionAlgorithmCamellia192GCM) // Camellia-192-GCM
db.UpsertSafeBox("data11", data, key, op.EncryptionAlgorithmCamellia256GCM) // Camellia-256-GCM

// ARIA variants (Korean standard, government-approved)
db.UpsertSafeBox("data12", data, key, op.EncryptionAlgorithmARIA128GCM)  // ARIA-128-GCM
db.UpsertSafeBox("data13", data, key, op.EncryptionAlgorithmARIA192GCM)  // ARIA-192-GCM
db.UpsertSafeBox("data14", data, key, op.EncryptionAlgorithmARIA256GCM)  // ARIA-256-GCM

// No encryption (for testing or when encryption is handled externally)
db.UpsertSafeBox("data15", data, key, op.EncryptionAlgorithmNone)
```

### Algorithm Recommendations

| Algorithm | Use Case | Security Level | Performance |
|-----------|----------|----------------|-------------|
| **AES-256-GCM** | General purpose, high security | ⭐⭐⭐⭐⭐ Highest | ⭐⭐⭐⭐ Fast |
| **ChaCha20-Poly1305** | Mobile, ARM processors | ⭐⭐⭐⭐⭐ Highest | ⭐⭐⭐⭐⭐ Fastest |
| **XChaCha20-Poly1305** | Large nonces, long-lived data | ⭐⭐⭐⭐⭐ Highest | ⭐⭐⭐⭐⭐ Fastest |
| **ASCON-128** | IoT, constrained environments | ⭐⭐⭐⭐ High | ⭐⭐⭐⭐⭐ Very Fast |
| **ASCON-80pq** | Post-quantum security | ⭐⭐⭐⭐ High | ⭐⭐⭐⭐ Fast |
| **Camellia-256-GCM** | Government, compliance | ⭐⭐⭐⭐⭐ Highest | ⭐⭐⭐ Good |
| **ARIA-256-GCM** | Korean compliance | ⭐⭐⭐⭐⭐ Highest | ⭐⭐⭐ Good |

### Key Management

SafeBox uses BLAKE3 for key derivation, ensuring strong key material regardless of input quality:

```go
// Keys are automatically hashed to the correct length for each algorithm
shortKey := []byte("password")           // Any length key works
longKey := []byte("very-long-password-with-lots-of-entropy-here")

// Both will work - Tower handles key derivation internally
db.UpsertSafeBox("data1", data, shortKey, op.EncryptionAlgorithmAES256GCM)
db.UpsertSafeBox("data2", data, longKey, op.EncryptionAlgorithmChaCha20Poly1305)

// For maximum security, use high-entropy keys
import "crypto/rand"
secureKey := make([]byte, 32)
rand.Read(secureKey)
db.UpsertSafeBox("secure_data", sensitiveData, secureKey, op.EncryptionAlgorithmAES256GCM)
```

**Key Features:**
- ✅ **Multiple Algorithms**: 15 encryption algorithms including AES, ChaCha20, ASCON, Camellia, ARIA
- ✅ **Automatic Key Derivation**: BLAKE3-based key stretching for any input key length
- ✅ **Secure Nonces**: Cryptographically secure random nonce generation per operation
- ✅ **Algorithm Agility**: Easy to change encryption algorithms without data migration
- ✅ **Authenticated Encryption**: All algorithms provide built-in integrity protection
- ✅ **Thread-Safe**: All operations are atomic and concurrent-safe
- ✅ **Zero-Copy**: Efficient handling of large encrypted payloads

**Use Cases:**
- 🔐 **API Keys**: Secure storage of third-party service credentials
- 🏛️ **Compliance**: Government and industry-standard encryption algorithms
- 📱 **Mobile Apps**: Secure user data storage with ChaCha20-Poly1305
- 🌐 **IoT Devices**: Lightweight encryption with ASCON algorithms
- 💼 **Enterprise**: Configuration secrets, database credentials
- 🔄 **Key Rotation**: Seamless encryption algorithm upgrades
- 🛡️ **Data Protection**: GDPR/HIPAA compliant encrypted storage

## ⚙️ Configuration

### Storage Options

All storage options are configured via `op.Options`.

```go
import (
    "github.com/rivulet-io/tower/op"
    "github.com/rivulet-io/tower/util/size"
)

// In-memory storage (for testing/caching)
opts := &op.Options{
    FS:           op.InMemory(),
    BytesPerSync: size.NewSizeFromKilobytes(1),
    CacheSize:    size.NewSizeFromMegabytes(10),
    MemTableSize: size.NewSizeFromMegabytes(5),
}

// Persistent disk storage
opts := &op.Options{
    Path:         "/path/to/database",        // Optional: custom path
    FS:           op.OnDisk(),
    BytesPerSync: size.NewSizeFromKilobytes(64),
    CacheSize:    size.NewSizeFromGigabytes(1),
    MemTableSize: size.NewSizeFromMegabytes(64),
}
```

### Size Utilities

Tower provides size constructors and conversion methods via the `util/size` package:

```go
import "github.com/rivulet-io/tower/util/size"

// Size constructors
s := size.NewSizeFromBytes(1024)       // 1024 bytes
s = size.NewSizeFromKilobytes(1)       // 1 KB  
s = size.NewSizeFromMegabytes(10)      // 10 MB
s = size.NewSizeFromGigabytes(1)       // 1 GB
s = size.NewSizeFromTerabytes(1)       // 1 TB

// Size conversions
bytes := s.Bytes()                      // Get as int64 bytes
kb := s.Kilobytes()                     // Get as float64 KB
mb := s.Megabytes()                     // Get as float64 MB
gb := s.Gigabytes()                     // Get as float64 GB

// String representation with automatic unit selection
fmt.Println(s.String())                 // "1.00 GB"
```

## 🧪 Testing

Tower includes comprehensive test coverage for all operations in both the `op` and `mesh` packages.

```bash
# Run all tests
go test ./...

# Run tests for a specific package
go test ./op -v
go test ./mesh -v
```

The test suite is designed to validate:
- **Correctness**: All operations produce expected results.
- **Atomicity**: Operations are atomic and consistent.
- **Concurrency**: Thread-safe access patterns are enforced.
- **Error Handling**: Proper error propagation and edge cases are handled.
- **Distributed Scenarios (`mesh` package)**: The `mesh` tests are particularly noteworthy, simulating realistic multi-node clusters, gateway connections, and leaf node interactions. They verify complex behaviors such as data replication, distributed locking, and message routing in a clustered environment, ensuring the reliability of the distributed system.

## 🚦 Concurrency

The `op.Operator` is architected for high-concurrency scenarios:

### Locking Strategy
- **Per-key locking**: Each key has its own RWMutex, minimizing contention.
- **Fine-grained locks**: Operations only lock specific keys, not the entire database.
- **Read-write separation**: Read operations use read locks, allowing concurrent reads.

### Thread Safety
```go
import "sync"

// All operations on op.Operator are thread-safe
func concurrentExample(db *op.Operator) {
    var wg sync.WaitGroup
    
    // Concurrent writes to different keys - no contention
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()

            key := fmt.Sprintf("key_%d", id)
            db.SetInt(key, int64(id))
        }(i)
    }
    
    // Concurrent reads - no blocking
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            key := fmt.Sprintf("key_%d", id)
            db.GetInt(key)
        }(i)
    }
    
    wg.Wait()
}
```

## 📊 Performance

Tower is optimized for high-performance workloads with multiple optimization strategies:

### Storage Engine
- **Pebble LSM-tree**: Optimized for high write throughput and range queries.
- **Write amplification**: Minimized through efficient compaction strategies.
- **Read amplification**: Reduced via bloom filters and efficient caching.

### Memory Management
- **Configurable cache**: Tune cache size based on working set and available memory.
- **Memory tables**: Adjustable MemTable size for write buffering.
- **Compression**: Built-in compression reduces storage footprint.

```go
// Performance-tuned configuration for high-throughput workloads
opts := &op.Options{
    FS:           op.OnDisk(),
    BytesPerSync: size.NewSizeFromMegabytes(1),     // Larger sync intervals
    CacheSize:    size.NewSizeFromGigabytes(2),     // Large cache for hot data
    MemTableSize: size.NewSizeFromMegabytes(128),   // Large write buffer
}
```

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
- **Code Style**: Follow `gofmt` and `golint` standards.
- **Testing**: All new features must include comprehensive tests.
- **Documentation**: Update README and add code comments for public APIs.
- **Commits**: Use conventional commit format for clear history.

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
