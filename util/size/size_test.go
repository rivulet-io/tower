package size

import (
	"testing"
)

func TestSizeConstructors(t *testing.T) {
	tests := []struct {
		name     string
		fn       func(float64) Size
		input    float64
		expected Size
	}{
		{
			name:     "NewSizeFromKilobytes",
			fn:       NewSizeFromKilobytes,
			input:    1,
			expected: Size(1024),
		},
		{
			name:     "NewSizeFromMegabytes",
			fn:       NewSizeFromMegabytes,
			input:    1,
			expected: Size(1024 * 1024),
		},
		{
			name:     "NewSizeFromGigabytes",
			fn:       NewSizeFromGigabytes,
			input:    1,
			expected: Size(1024 * 1024 * 1024),
		},
		{
			name:     "NewSizeFromTerabytes",
			fn:       NewSizeFromTerabytes,
			input:    1,
			expected: Size(1024 * 1024 * 1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.fn(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}

	// Test NewSizeFromBytes separately since it takes int64
	t.Run("NewSizeFromBytes", func(t *testing.T) {
		result := NewSizeFromBytes(1024)
		expected := Size(1024)
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	})
}

func TestSizeString(t *testing.T) {
	tests := []struct {
		name     string
		size     Size
		expected string
	}{
		{
			name:     "bytes",
			size:     Size(512),
			expected: "512 B",
		},
		{
			name:     "kilobytes",
			size:     NewSizeFromKilobytes(1),
			expected: "1.00 KB",
		},
		{
			name:     "megabytes",
			size:     NewSizeFromMegabytes(1),
			expected: "1.00 MB",
		},
		{
			name:     "gigabytes",
			size:     NewSizeFromGigabytes(1),
			expected: "1.00 GB",
		},
		{
			name:     "terabytes",
			size:     NewSizeFromTerabytes(1),
			expected: "1.00 TB",
		},
		{
			name:     "mixed size",
			size:     NewSizeFromMegabytes(1536), // 1.5 GB
			expected: "1.50 GB",
		},
		{
			name:     "fractional KB",
			size:     NewSizeFromBytes(1536), // 1.5 KB
			expected: "1.50 KB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.size.String()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestSizeBytes(t *testing.T) {
	tests := []struct {
		name     string
		size     Size
		expected int64
	}{
		{
			name:     "bytes",
			size:     Size(1024),
			expected: 1024,
		},
		{
			name:     "kilobytes",
			size:     NewSizeFromKilobytes(2),
			expected: 2048,
		},
		{
			name:     "megabytes",
			size:     NewSizeFromMegabytes(1),
			expected: 1024 * 1024,
		},
		{
			name:     "gigabytes",
			size:     NewSizeFromGigabytes(1),
			expected: 1024 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.size.Bytes()
			if result != tt.expected {
				t.Errorf("Expected %d bytes, got %d", tt.expected, result)
			}
		})
	}
}

func TestSizeKilobytes(t *testing.T) {
	tests := []struct {
		name     string
		size     Size
		expected float64
	}{
		{
			name:     "exact kilobytes",
			size:     NewSizeFromKilobytes(5),
			expected: 5,
		},
		{
			name:     "from bytes",
			size:     Size(2048),
			expected: 2,
		},
		{
			name:     "from megabytes",
			size:     NewSizeFromMegabytes(1),
			expected: 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.size.Kilobytes()
			if result != tt.expected {
				t.Errorf("Expected %f KB, got %f", tt.expected, result)
			}
		})
	}
}

func TestSizeMegabytes(t *testing.T) {
	tests := []struct {
		name     string
		size     Size
		expected float64
	}{
		{
			name:     "exact megabytes",
			size:     NewSizeFromMegabytes(10),
			expected: 10,
		},
		{
			name:     "from kilobytes",
			size:     NewSizeFromKilobytes(2048),
			expected: 2,
		},
		{
			name:     "from gigabytes",
			size:     NewSizeFromGigabytes(1),
			expected: 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.size.Megabytes()
			if result != tt.expected {
				t.Errorf("Expected %f MB, got %f", tt.expected, result)
			}
		})
	}
}

func TestSizeGigabytes(t *testing.T) {
	tests := []struct {
		name     string
		size     Size
		expected float64
	}{
		{
			name:     "exact gigabytes",
			size:     NewSizeFromGigabytes(5),
			expected: 5,
		},
		{
			name:     "from megabytes",
			size:     NewSizeFromMegabytes(3072),
			expected: 3,
		},
		{
			name:     "from terabytes",
			size:     NewSizeFromTerabytes(1),
			expected: 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.size.Gigabytes()
			if result != tt.expected {
				t.Errorf("Expected %f GB, got %f", tt.expected, result)
			}
		})
	}
}

func TestSizeTerabytes(t *testing.T) {
	tests := []struct {
		name     string
		size     Size
		expected float64
	}{
		{
			name:     "exact terabytes",
			size:     NewSizeFromTerabytes(2),
			expected: 2,
		},
		{
			name:     "from gigabytes",
			size:     NewSizeFromGigabytes(2048),
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.size.Terabytes()
			if result != tt.expected {
				t.Errorf("Expected %f TB, got %f", tt.expected, result)
			}
		})
	}
}

func TestSizeMarshalText(t *testing.T) {
	tests := []struct {
		name     string
		size     Size
		expected string
	}{
		{
			name:     "bytes",
			size:     Size(256),
			expected: "256 B",
		},
		{
			name:     "kilobytes",
			size:     NewSizeFromKilobytes(1),
			expected: "1.00 KB",
		},
		{
			name:     "megabytes",
			size:     NewSizeFromMegabytes(64),
			expected: "64.00 MB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.size.MarshalText()
			if err != nil {
				t.Errorf("MarshalText failed: %v", err)
			}
			if string(result) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(result))
			}
		})
	}
}

func TestSizeUnmarshalText(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Size
		wantErr  bool
	}{
		{
			name:     "bytes",
			input:    "512 B",
			expected: Size(512),
			wantErr:  false,
		},
		{
			name:     "kilobytes",
			input:    "2.00 KB",
			expected: NewSizeFromKilobytes(2),
			wantErr:  false,
		},
		{
			name:     "megabytes",
			input:    "1.50 MB",
			expected: Size(1.5 * 1024 * 1024),
			wantErr:  false,
		},
		{
			name:     "gigabytes",
			input:    "3.00 GB",
			expected: NewSizeFromGigabytes(3),
			wantErr:  false,
		},
		{
			name:     "invalid format",
			input:    "invalid",
			expected: Size(0),
			wantErr:  true,
		},
		{
			name:     "unknown unit",
			input:    "100 XB",
			expected: Size(0),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var size Size
			err := size.UnmarshalText([]byte(tt.input))

			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalText() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && size != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, size)
			}
		})
	}
}

func TestSizeConstants(t *testing.T) {
	tests := []struct {
		name     string
		constant Size
		expected int64
	}{
		{
			name:     "SizeBytes",
			constant: SizeBytes,
			expected: 1,
		},
		{
			name:     "SizeKilobytes",
			constant: SizeKilobytes,
			expected: 1024,
		},
		{
			name:     "SizeMegabytes",
			constant: SizeMegabytes,
			expected: 1024 * 1024,
		},
		{
			name:     "SizeGigabytes",
			constant: SizeGigabytes,
			expected: 1024 * 1024 * 1024,
		},
		{
			name:     "SizeTerabytes",
			constant: SizeTerabytes,
			expected: 1024 * 1024 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int64(tt.constant) != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, int64(tt.constant))
			}
		})
	}
}
