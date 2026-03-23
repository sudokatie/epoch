package compress

import (
	"math"
)

// Gorilla compression implements Facebook's Gorilla paper compression
// for time series data. Optimized for:
// - Timestamps: delta-of-delta encoding with variable bit packing
// - Floats: XOR with previous value, leading/trailing zero compression

// CompressTimestamps compresses a slice of nanosecond timestamps
// using delta-of-delta encoding with variable bit packing
func CompressTimestamps(timestamps []int64) []byte {
	if len(timestamps) == 0 {
		return nil
	}

	w := NewBitWriter()

	// Write count as 4 bytes
	w.WriteBits(uint64(len(timestamps)), 32)

	// First timestamp stored as-is (64 bits)
	EncodeInt64(w, timestamps[0])

	if len(timestamps) == 1 {
		return w.Bytes()
	}

	// Second stored as delta from first (64 bits for simplicity in header)
	delta := timestamps[1] - timestamps[0]
	EncodeInt64(w, delta)

	prevDelta := delta

	// Subsequent as delta-of-deltas with variable bit packing
	for i := 2; i < len(timestamps); i++ {
		delta = timestamps[i] - timestamps[i-1]
		dod := delta - prevDelta

		if dod == 0 {
			// Single 0 bit
			w.WriteBit(false)
		} else if dod >= -63 && dod <= 64 {
			// 10 prefix + 7 bits for value in [-63, 64]
			w.WriteBit(true)
			w.WriteBit(false)
			// Encode as unsigned with bias
			w.WriteBits(uint64(dod+63), 7)
		} else if dod >= -255 && dod <= 256 {
			// 110 prefix + 9 bits for value in [-255, 256]
			w.WriteBit(true)
			w.WriteBit(true)
			w.WriteBit(false)
			w.WriteBits(uint64(dod+255), 9)
		} else if dod >= -2047 && dod <= 2048 {
			// 1110 prefix + 12 bits for value in [-2047, 2048]
			w.WriteBit(true)
			w.WriteBit(true)
			w.WriteBit(true)
			w.WriteBit(false)
			w.WriteBits(uint64(dod+2047), 12)
		} else {
			// 1111 prefix + 64 bits for larger values
			w.WriteBit(true)
			w.WriteBit(true)
			w.WriteBit(true)
			w.WriteBit(true)
			// Use 64 bits for full precision
			EncodeInt64(w, dod)
		}

		prevDelta = delta
	}

	return w.Bytes()
}

// DecompressTimestamps decompresses Gorilla-encoded timestamps
func DecompressTimestamps(data []byte) ([]int64, error) {
	if len(data) == 0 {
		return nil, nil
	}

	r := NewBitReader(data)

	// Read count
	count, err := r.ReadBits(32)
	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, nil
	}

	timestamps := make([]int64, count)

	// First timestamp
	timestamps[0], err = DecodeInt64(r)
	if err != nil {
		return nil, err
	}

	if count == 1 {
		return timestamps, nil
	}

	// Second as delta
	delta, err := DecodeInt64(r)
	if err != nil {
		return nil, err
	}
	timestamps[1] = timestamps[0] + delta

	prevDelta := delta

	// Rest as delta-of-deltas
	for i := uint64(2); i < count; i++ {
		var dod int64

		bit, err := r.ReadBit()
		if err != nil {
			return nil, err
		}

		if !bit {
			// 0 bit: dod is 0
			dod = 0
		} else {
			bit, err = r.ReadBit()
			if err != nil {
				return nil, err
			}

			if !bit {
				// 10: 7 bits
				v, err := r.ReadBits(7)
				if err != nil {
					return nil, err
				}
				dod = int64(v) - 63
			} else {
				bit, err = r.ReadBit()
				if err != nil {
					return nil, err
				}

				if !bit {
					// 110: 9 bits
					v, err := r.ReadBits(9)
					if err != nil {
						return nil, err
					}
					dod = int64(v) - 255
				} else {
					bit, err = r.ReadBit()
					if err != nil {
						return nil, err
					}

					if !bit {
						// 1110: 12 bits
						v, err := r.ReadBits(12)
						if err != nil {
							return nil, err
						}
						dod = int64(v) - 2047
					} else {
						// 1111: 64 bits
						dod, err = DecodeInt64(r)
						if err != nil {
							return nil, err
						}
					}
				}
			}
		}

		delta = prevDelta + dod
		timestamps[i] = timestamps[i-1] + delta
		prevDelta = delta
	}

	return timestamps, nil
}

// CompressFloats compresses a slice of float64 values using XOR encoding
// with leading/trailing zero compression
func CompressFloats(values []float64) []byte {
	if len(values) == 0 {
		return nil
	}

	w := NewBitWriter()

	// Write count as 4 bytes
	w.WriteBits(uint64(len(values)), 32)

	// First value stored as-is (64 bits)
	EncodeUint64(w, math.Float64bits(values[0]))

	if len(values) == 1 {
		return w.Bytes()
	}

	prevValue := math.Float64bits(values[0])
	prevLeading := uint8(0)
	prevTrailing := uint8(0)

	for i := 1; i < len(values); i++ {
		curr := math.Float64bits(values[i])
		xor := prevValue ^ curr

		if xor == 0 {
			// Same value: single 0 bit
			w.WriteBit(false)
		} else {
			// Different value: 1 bit
			w.WriteBit(true)

			leading := uint8(countLeadingZeros64(xor))
			trailing := uint8(countTrailingZeros64(xor))

			// Cap leading zeros at 31 (5 bits)
			if leading > 31 {
				leading = 31
			}

			meaningfulBits := 64 - leading - trailing

			// Check if we can reuse the previous window
			if prevLeading > 0 && leading >= prevLeading && trailing >= prevTrailing {
				// 10 prefix: reuse previous leading/trailing
				w.WriteBit(false)
				// Write only the meaningful bits using previous window
				w.WriteBits(xor>>(64-prevLeading-uint8(64-int(prevLeading)-int(prevTrailing))),
					uint8(64-int(prevLeading)-int(prevTrailing)))
			} else {
				// 11 prefix: new leading/trailing
				w.WriteBit(true)
				// 5 bits for leading zeros count
				w.WriteBits(uint64(leading), 5)
				// 6 bits for meaningful bits count (0-63, so we encode count-1 would overflow; use count directly)
				w.WriteBits(uint64(meaningfulBits), 6)
				// Write meaningful bits
				w.WriteBits(xor>>trailing, meaningfulBits)

				prevLeading = leading
				prevTrailing = trailing
			}
		}

		prevValue = curr
	}

	return w.Bytes()
}

// DecompressFloats decompresses XOR-encoded float64 values
func DecompressFloats(data []byte) ([]float64, error) {
	if len(data) == 0 {
		return nil, nil
	}

	r := NewBitReader(data)

	// Read count
	count, err := r.ReadBits(32)
	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, nil
	}

	values := make([]float64, count)

	// First value
	bits, err := DecodeUint64(r)
	if err != nil {
		return nil, err
	}
	values[0] = math.Float64frombits(bits)

	if count == 1 {
		return values, nil
	}

	prevValue := bits
	prevLeading := uint8(0)
	prevMeaningful := uint8(64)

	for i := uint64(1); i < count; i++ {
		bit, err := r.ReadBit()
		if err != nil {
			return nil, err
		}

		if !bit {
			// Same as previous
			values[i] = math.Float64frombits(prevValue)
		} else {
			bit, err = r.ReadBit()
			if err != nil {
				return nil, err
			}

			var xor uint64

			if !bit {
				// 10: reuse previous window
				xorBits, err := r.ReadBits(prevMeaningful)
				if err != nil {
					return nil, err
				}
				trailing := 64 - prevLeading - prevMeaningful
				xor = xorBits << trailing
			} else {
				// 11: new window
				leading, err := r.ReadBits(5)
				if err != nil {
					return nil, err
				}
				meaningful, err := r.ReadBits(6)
				if err != nil {
					return nil, err
				}

				xorBits, err := r.ReadBits(uint8(meaningful))
				if err != nil {
					return nil, err
				}

				trailing := 64 - uint8(leading) - uint8(meaningful)
				xor = xorBits << trailing

				prevLeading = uint8(leading)
				prevMeaningful = uint8(meaningful)
			}

			curr := prevValue ^ xor
			values[i] = math.Float64frombits(curr)
			prevValue = curr
		}
	}

	return values, nil
}

// countLeadingZeros64 counts leading zeros in a uint64
func countLeadingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	if x <= 0x00000000FFFFFFFF {
		n += 32
		x <<= 32
	}
	if x <= 0x0000FFFFFFFFFFFF {
		n += 16
		x <<= 16
	}
	if x <= 0x00FFFFFFFFFFFFFF {
		n += 8
		x <<= 8
	}
	if x <= 0x0FFFFFFFFFFFFFFF {
		n += 4
		x <<= 4
	}
	if x <= 0x3FFFFFFFFFFFFFFF {
		n += 2
		x <<= 2
	}
	if x <= 0x7FFFFFFFFFFFFFFF {
		n += 1
	}
	return n
}

// countTrailingZeros64 counts trailing zeros in a uint64
func countTrailingZeros64(x uint64) int {
	if x == 0 {
		return 64
	}
	n := 0
	if x&0x00000000FFFFFFFF == 0 {
		n += 32
		x >>= 32
	}
	if x&0x000000000000FFFF == 0 {
		n += 16
		x >>= 16
	}
	if x&0x00000000000000FF == 0 {
		n += 8
		x >>= 8
	}
	if x&0x000000000000000F == 0 {
		n += 4
		x >>= 4
	}
	if x&0x0000000000000003 == 0 {
		n += 2
		x >>= 2
	}
	if x&0x0000000000000001 == 0 {
		n += 1
	}
	return n
}

// CompressIntegers compresses a slice of int64 values using delta encoding
func CompressIntegers(values []int64) []byte {
	if len(values) == 0 {
		return nil
	}

	w := NewBitWriter()

	// Write count
	w.WriteBits(uint64(len(values)), 32)

	// First value as-is
	EncodeInt64(w, values[0])

	if len(values) == 1 {
		return w.Bytes()
	}

	// Rest as deltas with variable encoding
	prev := values[0]
	for i := 1; i < len(values); i++ {
		delta := values[i] - prev

		if delta == 0 {
			w.WriteBit(false)
		} else if delta >= -63 && delta <= 64 {
			w.WriteBit(true)
			w.WriteBit(false)
			w.WriteBits(uint64(delta+63), 7)
		} else if delta >= -8191 && delta <= 8192 {
			w.WriteBit(true)
			w.WriteBit(true)
			w.WriteBit(false)
			w.WriteBits(uint64(delta+8191), 14)
		} else {
			w.WriteBit(true)
			w.WriteBit(true)
			w.WriteBit(true)
			EncodeInt64(w, delta)
		}

		prev = values[i]
	}

	return w.Bytes()
}

// DecompressIntegers decompresses delta-encoded int64 values
func DecompressIntegers(data []byte) ([]int64, error) {
	if len(data) == 0 {
		return nil, nil
	}

	r := NewBitReader(data)

	count, err := r.ReadBits(32)
	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, nil
	}

	values := make([]int64, count)

	values[0], err = DecodeInt64(r)
	if err != nil {
		return nil, err
	}

	if count == 1 {
		return values, nil
	}

	prev := values[0]
	for i := uint64(1); i < count; i++ {
		var delta int64

		bit, err := r.ReadBit()
		if err != nil {
			return nil, err
		}

		if !bit {
			delta = 0
		} else {
			bit, err = r.ReadBit()
			if err != nil {
				return nil, err
			}

			if !bit {
				v, err := r.ReadBits(7)
				if err != nil {
					return nil, err
				}
				delta = int64(v) - 63
			} else {
				bit, err = r.ReadBit()
				if err != nil {
					return nil, err
				}

				if !bit {
					v, err := r.ReadBits(14)
					if err != nil {
						return nil, err
					}
					delta = int64(v) - 8191
				} else {
					delta, err = DecodeInt64(r)
					if err != nil {
						return nil, err
					}
				}
			}
		}

		values[i] = prev + delta
		prev = values[i]
	}

	return values, nil
}
