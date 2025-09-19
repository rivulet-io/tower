package op

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDataFrameCreation(t *testing.T) {
	tests := []struct {
		name     string
		creator  func() *DataFrame
		expected DataType
	}{
		{
			name: "string dataframe",
			creator: func() *DataFrame {
				df := &DataFrame{}
				df.SetString("test")
				return df
			},
			expected: TypeString,
		},
		{
			name: "int dataframe",
			creator: func() *DataFrame {
				df := &DataFrame{}
				df.SetInt(42)
				return df
			},
			expected: TypeInt,
		},
		{
			name: "float dataframe",
			creator: func() *DataFrame {
				df := &DataFrame{}
				df.SetFloat(3.14)
				return df
			},
			expected: TypeFloat,
		},
		{
			name: "bool dataframe",
			creator: func() *DataFrame {
				df := &DataFrame{}
				df.SetBool(true)
				return df
			},
			expected: TypeBool,
		},
		{
			name: "timestamp dataframe",
			creator: func() *DataFrame {
				df := &DataFrame{}
				df.SetTimestamp(time.Now())
				return df
			},
			expected: TypeTimestamp,
		},
		{
			name: "duration dataframe",
			creator: func() *DataFrame {
				df := &DataFrame{}
				df.SetDuration(time.Hour)
				return df
			},
			expected: TypeDuration,
		},
		{
			name: "uuid dataframe",
			creator: func() *DataFrame {
				df := &DataFrame{}
				id := uuid.New()
				df.SetUUID(&id)
				return df
			},
			expected: TypeUUID,
		},
		{
			name:     "null dataframe",
			creator:  func() *DataFrame { return NULLDataFrame() },
			expected: TypeNull,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := tt.creator()
			if df.Type() != tt.expected {
				t.Errorf("Expected type %v, got %v", tt.expected, df.Type())
			}
		})
	}
}

func TestDataFrameMarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name string
		df   *DataFrame
	}{
		{
			name: "string",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetString("hello world")
				return df
			}(),
		},
		{
			name: "int",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetInt(12345)
				return df
			}(),
		},
		{
			name: "float",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetFloat(3.14159)
				return df
			}(),
		},
		{
			name: "bool true",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetBool(true)
				return df
			}(),
		},
		{
			name: "bool false",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetBool(false)
				return df
			}(),
		},
		{
			name: "timestamp",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetTimestamp(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC))
				return df
			}(),
		},
		{
			name: "duration",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetDuration(2*time.Hour + 30*time.Minute)
				return df
			}(),
		},
		{
			name: "uuid",
			df: func() *DataFrame {
				df := &DataFrame{}
				id := uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
				df.SetUUID(&id)
				return df
			}(),
		},
		{
			name: "null",
			df:   NULLDataFrame(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data, err := tt.df.Marshal()
			if err != nil {
				t.Errorf("Marshal failed: %v", err)
				return
			}

			// Unmarshal
			df2, err := UnmarshalDataFrame(data)
			if err != nil {
				t.Errorf("Unmarshal failed: %v", err)
				return
			}

			// Compare types
			if df2.Type() != tt.df.Type() {
				t.Errorf("Types don't match: expected %v, got %v", tt.df.Type(), df2.Type())
				return
			}

			// Compare values based on type
			switch tt.df.Type() {
			case TypeString:
				orig, _ := tt.df.String()
				unmarshaled, _ := df2.String()
				if orig != unmarshaled {
					t.Errorf("String values don't match: expected %s, got %s", orig, unmarshaled)
				}
			case TypeInt:
				orig, _ := tt.df.Int()
				unmarshaled, _ := df2.Int()
				if orig != unmarshaled {
					t.Errorf("Int values don't match: expected %d, got %d", orig, unmarshaled)
				}
			case TypeFloat:
				orig, _ := tt.df.Float()
				unmarshaled, _ := df2.Float()
				if orig != unmarshaled {
					t.Errorf("Float values don't match: expected %f, got %f", orig, unmarshaled)
				}
			case TypeBool:
				orig, _ := tt.df.Bool()
				unmarshaled, _ := df2.Bool()
				if orig != unmarshaled {
					t.Errorf("Bool values don't match: expected %t, got %t", orig, unmarshaled)
				}
			case TypeTimestamp:
				orig, _ := tt.df.Timestamp()
				unmarshaled, _ := df2.Timestamp()
				if !orig.Equal(unmarshaled) {
					t.Errorf("Timestamp values don't match: expected %v, got %v", orig, unmarshaled)
				}
			case TypeDuration:
				orig, _ := tt.df.Duration()
				unmarshaled, _ := df2.Duration()
				if orig != unmarshaled {
					t.Errorf("Duration values don't match: expected %v, got %v", orig, unmarshaled)
				}
			case TypeUUID:
				orig, _ := tt.df.UUID()
				unmarshaled, _ := df2.UUID()
				if *orig != *unmarshaled {
					t.Errorf("UUID values don't match: expected %v, got %v", *orig, *unmarshaled)
				}
			case TypeNull:
				// Null values don't need comparison
			}
		})
	}
}

func TestDataFrameTypeConversion(t *testing.T) {
	// Test string conversion
	t.Run("string conversions", func(t *testing.T) {
		df := &DataFrame{}
		df.SetString("hello")
		val, err := df.String()
		if err != nil {
			t.Errorf("String conversion failed: %v", err)
		}
		if val != "hello" {
			t.Errorf("Expected 'hello', got '%s'", val)
		}

		// Test wrong type conversion
		_, err = df.Int()
		if err == nil {
			t.Error("Expected error when converting string to int")
		}
	})

	// Test int conversion
	t.Run("int conversions", func(t *testing.T) {
		df := &DataFrame{}
		df.SetInt(42)
		val, err := df.Int()
		if err != nil {
			t.Errorf("Int conversion failed: %v", err)
		}
		if val != 42 {
			t.Errorf("Expected 42, got %d", val)
		}

		// Test wrong type conversion
		_, err = df.String()
		if err == nil {
			t.Error("Expected error when converting int to string")
		}
	})

	// Test float conversion
	t.Run("float conversions", func(t *testing.T) {
		df := &DataFrame{}
		df.SetFloat(3.14)
		val, err := df.Float()
		if err != nil {
			t.Errorf("Float conversion failed: %v", err)
		}
		if val != 3.14 {
			t.Errorf("Expected 3.14, got %f", val)
		}
	})

	// Test bool conversion
	t.Run("bool conversions", func(t *testing.T) {
		df := &DataFrame{}
		df.SetBool(true)
		val, err := df.Bool()
		if err != nil {
			t.Errorf("Bool conversion failed: %v", err)
		}
		if val != true {
			t.Errorf("Expected true, got %t", val)
		}
	})
}

func TestDataFrameIsNull(t *testing.T) {
	tests := []struct {
		name     string
		df       *DataFrame
		expected bool
	}{
		{
			name:     "null dataframe",
			df:       NULLDataFrame(),
			expected: true,
		},
		{
			name: "string dataframe",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetString("test")
				return df
			}(),
			expected: false,
		},
		{
			name: "int dataframe",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetInt(42)
				return df
			}(),
			expected: false,
		},
		{
			name: "float dataframe",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetFloat(3.14)
				return df
			}(),
			expected: false,
		},
		{
			name: "bool dataframe",
			df: func() *DataFrame {
				df := &DataFrame{}
				df.SetBool(false)
				return df
			}(),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.df.Type() == TypeNull
			if result != tt.expected {
				t.Errorf("IsNull() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestDataFrameError(t *testing.T) {
	err := &DataFrameError{
		Op:   "test",
		Type: TypeDecimal,
		Msg:  "test error",
	}

	expected := "dataframe test error for type 3: test error"
	if err.Error() != expected {
		t.Errorf("Error message = %s, expected %s", err.Error(), expected)
	}
}
