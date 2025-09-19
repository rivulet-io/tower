package benchmarks

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/rivulet-io/tower/op"
	"github.com/rivulet-io/tower/util/size"
)

// setupTowerForDecimalBenchmark creates an in-memory tower for Decimal benchmarking
func setupTowerForDecimalBenchmark(b *testing.B) *op.Operator {
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

// Benchmark basic Decimal operations
func BenchmarkSetDecimal(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Large decimal for realistic benchmarking
	coefficient, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
	scale := int32(10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("decimal:set:%d", i)
		if err := twr.SetDecimal(key, coefficient, scale); err != nil {
			b.Fatalf("SetDecimal failed: %v", err)
		}
	}
}

func BenchmarkGetDecimal(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "decimal:get:benchmark"
	coefficient, _ := new(big.Int).SetString("987654321098765432109876543210", 10)
	scale := int32(8)
	if err := twr.SetDecimal(key, coefficient, scale); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := twr.GetDecimal(key)
		if err != nil {
			b.Fatalf("GetDecimal failed: %v", err)
		}
	}
}

func BenchmarkAddDecimal(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "decimal:add:benchmark"
	baseCoeff, _ := new(big.Int).SetString("1000000000000000000000000000", 10)
	if err := twr.SetDecimal(key, baseCoeff, 10); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	addCoeff := big.NewInt(123456789)
	addScale := int32(5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := twr.AddDecimal(key, addCoeff, addScale)
		if err != nil {
			b.Fatalf("AddDecimal failed: %v", err)
		}
	}
}

func BenchmarkMulDecimal(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "decimal:mul:benchmark"
	baseCoeff, _ := new(big.Int).SetString("1000000000000000000000000", 10)
	if err := twr.SetDecimal(key, baseCoeff, 8); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	mulCoeff := big.NewInt(123456789)
	mulScale := int32(4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := twr.MulDecimal(key, mulCoeff, mulScale)
		if err != nil {
			b.Fatalf("MulDecimal failed: %v", err)
		}
	}
}

func BenchmarkDivDecimal(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "decimal:div:benchmark"
	baseCoeff, _ := new(big.Int).SetString("1000000000000000000000000000", 10) // Original large coefficient
	if err := twr.SetDecimal(key, baseCoeff, 10); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Verify data was stored correctly
	if coeff, scale, err := twr.GetDecimal(key); err != nil {
		b.Fatalf("GetDecimal failed after SetDecimal: %v", err)
	} else if coeff.Cmp(baseCoeff) != 0 || scale != 10 {
		b.Fatalf("Data mismatch: expected %s/%d, got %s/%d", baseCoeff.String(), 10, coeff.String(), scale)
	}

	divCoeff := big.NewInt(123456789)
	divScale := int32(5)
	resultScale := int32(15)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := twr.DivDecimal(key, divCoeff, divScale, resultScale)
		if err != nil {
			b.Fatalf("DivDecimal failed: %v", err)
		}
	}
}

func BenchmarkSubDecimal(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "decimal:sub:benchmark"
	baseCoeff, _ := new(big.Int).SetString("5000000000000000000000000000", 10)
	if err := twr.SetDecimal(key, baseCoeff, 10); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	subCoeff := big.NewInt(123456789)
	subScale := int32(5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := twr.SubDecimal(key, subCoeff, subScale)
		if err != nil {
			b.Fatalf("SubDecimal failed: %v", err)
		}
	}
}

// Benchmark financial calculations with high precision
func BenchmarkFinancialDecimal(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Setup financial data - large monetary values
	principal, _ := new(big.Int).SetString("1000000000000000000", 10) // $1,000,000,000.00
	if err := twr.SetDecimal("principal", principal, 2); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	interestRate := big.NewInt(525) // 5.25%

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Calculate interest: principal * rate / 100
		_, _, err := twr.MulDecimal("principal", interestRate, 2)
		if err != nil {
			b.Fatalf("Financial calculation failed: %v", err)
		}
	}
}

// Benchmark scientific calculations with different scales
func BenchmarkScientificDecimal(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Setup scientific data - physical constants
	planck, _ := new(big.Int).SetString("662607015", 10) // Planck constant (×10^-34)
	if err := twr.SetDecimal("planck", planck, 42); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	multiplier := big.NewInt(299792458) // Speed of light

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := twr.MulDecimal("planck", multiplier, 0)
		if err != nil {
			b.Fatalf("Scientific calculation failed: %v", err)
		}
	}
}

// Benchmark float conversion operations
func BenchmarkSetDecimalFromFloat(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	value := 123456789.123456789
	scale := int32(8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("decimal:float:%d", i)
		if err := twr.SetDecimalFromFloat(key, value, scale); err != nil {
			b.Fatalf("SetDecimalFromFloat failed: %v", err)
		}
	}
}

func BenchmarkGetDecimalAsFloat(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "decimal:asfloat:benchmark"
	coefficient, _ := new(big.Int).SetString("123456789123456789", 10)
	scale := int32(8)
	if err := twr.SetDecimal(key, coefficient, scale); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := twr.GetDecimalAsFloat(key)
		if err != nil {
			b.Fatalf("GetDecimalAsFloat failed: %v", err)
		}
	}
}

// Benchmark scale alignment operations
func BenchmarkScaleAlignmentDecimal(b *testing.B) {
	twr := setupTowerForDecimalBenchmark(b)
	defer twr.Close()

	// Setup data with different scales
	key1 := "decimal:scale1"
	key2 := "decimal:scale2"

	coeff1 := big.NewInt(123456789)
	coeff2 := big.NewInt(987654321)

	if err := twr.SetDecimal(key1, coeff1, 5); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}
	if err := twr.SetDecimal(key2, coeff2, 8); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// This will trigger scale alignment internally
		_, _, err := twr.AddDecimal(key1, coeff2, 8)
		if err != nil {
			b.Fatalf("Scale alignment failed: %v", err)
		}
	}
}
