package size

import "fmt"

type Size int64

const (
	SizeBytes     Size = 1
	SizeKilobytes      = 1024 * SizeBytes
	SizeMegabytes      = 1024 * SizeKilobytes
	SizeGigabytes      = 1024 * SizeMegabytes
	SizeTerabytes      = 1024 * SizeGigabytes
	SizePetabytes      = 1024 * SizeTerabytes
	SizeExabytes       = 1024 * SizePetabytes
)

func (s Size) String() string {
	switch {
	case s >= SizeExabytes:
		return fmt.Sprintf("%.2f EB", float64(s)/float64(SizeExabytes))
	case s >= SizePetabytes:
		return fmt.Sprintf("%.2f PB", float64(s)/float64(SizePetabytes))
	case s >= SizeTerabytes:
		return fmt.Sprintf("%.2f TB", float64(s)/float64(SizeTerabytes))
	case s >= SizeGigabytes:
		return fmt.Sprintf("%.2f GB", float64(s)/float64(SizeGigabytes))
	case s >= SizeMegabytes:
		return fmt.Sprintf("%.2f MB", float64(s)/float64(SizeMegabytes))
	case s >= SizeKilobytes:
		return fmt.Sprintf("%.2f KB", float64(s)/float64(SizeKilobytes))
	default:
		return fmt.Sprintf("%d B", s)
	}
}

func (s Size) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *Size) UnmarshalText(text []byte) error {
	var value float64
	var unit string
	_, err := fmt.Sscanf(string(text), "%f %s", &value, &unit)
	if err != nil {
		return fmt.Errorf("invalid size format: %w", err)
	}

	switch unit {
	case "B":
		*s = Size(value * float64(SizeBytes))
	case "KB":
		*s = Size(value * float64(SizeKilobytes))
	case "MB":
		*s = Size(value * float64(SizeMegabytes))
	case "GB":
		*s = Size(value * float64(SizeGigabytes))
	case "TB":
		*s = Size(value * float64(SizeTerabytes))
	case "PB":
		*s = Size(value * float64(SizePetabytes))
	case "EB":
		*s = Size(value * float64(SizeExabytes))
	default:
		return fmt.Errorf("unknown size unit: %s", unit)
	}
	return nil
}

func (s Size) Bytes() int64 {
	return int64(s)
}

func (s Size) Kilobytes() float64 {
	return float64(s) / float64(SizeKilobytes)
}

func (s Size) Megabytes() float64 {
	return float64(s) / float64(SizeMegabytes)
}

func (s Size) Gigabytes() float64 {
	return float64(s) / float64(SizeGigabytes)
}

func (s Size) Terabytes() float64 {
	return float64(s) / float64(SizeTerabytes)
}

func (s Size) Petabytes() float64 {
	return float64(s) / float64(SizePetabytes)
}

func (s Size) Exabytes() float64 {
	return float64(s) / float64(SizeExabytes)
}

func NewSizeFromBytes(bytes int64) Size {
	return Size(bytes)
}

func NewSizeFromKilobytes(kb float64) Size {
	return Size(kb * float64(SizeKilobytes))
}

func NewSizeFromMegabytes(mb float64) Size {
	return Size(mb * float64(SizeMegabytes))
}

func NewSizeFromGigabytes(gb float64) Size {
	return Size(gb * float64(SizeGigabytes))
}

func NewSizeFromTerabytes(tb float64) Size {
	return Size(tb * float64(SizeTerabytes))
}

func NewSizeFromPetabytes(pb float64) Size {
	return Size(pb * float64(SizePetabytes))
}

func NewSizeFromExabytes(eb float64) Size {
	return Size(eb * float64(SizeExabytes))
}
