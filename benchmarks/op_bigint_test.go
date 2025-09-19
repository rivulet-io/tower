package benchmarks

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/rivulet-io/tower/op"
	"github.com/rivulet-io/tower/util/size"
)

// setupTowerForBigIntBenchmark creates an in-memory tower for BigInt benchmarking
func setupTowerForBigIntBenchmark(b *testing.B) *op.Operator {
	b.Helper()
	twr, err := op.NewOperator(&op.Options{
		Path:         "benchmark_data",
		FS:           op.InMemory(),
		CacheSize:    size.NewSizeFromMegabytes(64),
		MemTableSize: size.NewSizeFromMegabytes(16),
		BytesPerSync: size.NewSizeFromKilobytes(512),
	})
	if err != nil {
		b.Fatalf("Failed to create tower: %v", err)
	}
	return twr
}

// Benchmark basic BigInt operations
func BenchmarkSetBigInt(b *testing.B) {
	twr := setupTowerForBigIntBenchmark(b)
	defer twr.Close()

	// Large number for realistic benchmarking
	bigNum, _ := new(big.Int).SetString("1234567890123456789012345678901234567890", 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bigint:set:%d", i)
		if err := twr.SetBigInt(key, bigNum); err != nil {
			b.Fatalf("SetBigInt failed: %v", err)
		}
	}
}

func BenchmarkGetBigInt(b *testing.B) {
	twr := setupTowerForBigIntBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "bigint:get:benchmark"
	bigNum, _ := new(big.Int).SetString("9876543210987654321098765432109876543210", 10)
	if err := twr.SetBigInt(key, bigNum); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := twr.GetBigInt(key)
		if err != nil {
			b.Fatalf("GetBigInt failed: %v", err)
		}
	}
}

func BenchmarkAddBigInt(b *testing.B) {
	twr := setupTowerForBigIntBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "bigint:add:benchmark"
	baseNum, _ := new(big.Int).SetString("1000000000000000000000000000000000000", 10)
	if err := twr.SetBigInt(key, baseNum); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	addNum := big.NewInt(123456789)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := twr.AddBigInt(key, addNum)
		if err != nil {
			b.Fatalf("AddBigInt failed: %v", err)
		}
	}
}

func BenchmarkMulBigInt(b *testing.B) {
	twr := setupTowerForBigIntBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "bigint:mul:benchmark"
	baseNum, _ := new(big.Int).SetString("1000000000000000000000000000000", 10)
	if err := twr.SetBigInt(key, baseNum); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	mulNum := big.NewInt(123456789)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := twr.MulBigInt(key, mulNum)
		if err != nil {
			b.Fatalf("MulBigInt failed: %v", err)
		}
	}
}

func BenchmarkDivBigInt(b *testing.B) {
	twr := setupTowerForBigIntBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "bigint:div:benchmark"
	baseNum, _ := new(big.Int).SetString("1000000000000000000000000000000000000", 10)
	if err := twr.SetBigInt(key, baseNum); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	divNum := big.NewInt(123456789)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := twr.DivBigInt(key, divNum)
		if err != nil {
			b.Fatalf("DivBigInt failed: %v", err)
		}
	}
}

func BenchmarkCmpBigInt(b *testing.B) {
	twr := setupTowerForBigIntBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "bigint:cmp:benchmark"
	bigNum, _ := new(big.Int).SetString("500000000000000000000000000000000000", 10)
	if err := twr.SetBigInt(key, bigNum); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	cmpNum, _ := new(big.Int).SetString("600000000000000000000000000000000000", 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := twr.CmpBigInt(key, cmpNum)
		if err != nil {
			b.Fatalf("CmpBigInt failed: %v", err)
		}
	}
}

// Benchmark cryptographic operations with very large numbers
func BenchmarkCryptoBigInt(b *testing.B) {
	twr := setupTowerForBigIntBenchmark(b)
	defer twr.Close()

	// Large prime-like number (1024-bit)
	largeNum, _ := new(big.Int).SetString("179769313486231590772930519078902473361797697894230657273430081157732675805500963132708477322407536021120113879871393357658789768814416622492847430639474124377767893424865485276302219601246094119453082952085005768838150682342462881473913110540827237163350510684586298239947245938479716304835356329624224137859", 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bigint:crypto:%d", i)
		if err := twr.SetBigInt(key, largeNum); err != nil {
			b.Fatalf("SetBigInt crypto failed: %v", err)
		}
	}
}

// Benchmark scientific computing with astronomical numbers
func BenchmarkScientificBigInt(b *testing.B) {
	twr := setupTowerForBigIntBenchmark(b)
	defer twr.Close()

	// Avogadro's number approximation
	avogadro, _ := new(big.Int).SetString("602214076000000000000000", 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bigint:science:%d", i)
		if err := twr.SetBigInt(key, avogadro); err != nil {
			b.Fatalf("SetBigInt science failed: %v", err)
		}
	}
}
