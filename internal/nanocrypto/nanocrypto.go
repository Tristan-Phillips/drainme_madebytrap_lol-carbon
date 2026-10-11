// Package nanocrypto is Nano's key derivation, addresses, state-block hashing, Ed25519-Blake2b signing and
// work difficulty, done in-process so the private key never leaves drainme (README §3).
package nanocrypto

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"filippo.io/edwards25519"
	"golang.org/x/crypto/blake2b"
)

type (
	PublicKey [32]byte
	Hash      [32]byte
	Signature [64]byte
)

// PrivateKey is opaque so its bytes cannot reach a log or error through fmt.
type PrivateKey struct{ b [32]byte }

func (PrivateKey) Format(f fmt.State, _ rune) { fmt.Fprint(f, "PrivateKey(redacted)") }

var (
	ErrAddress = errors.New("nanocrypto: invalid nano_ address")
	ErrBalance = errors.New("nanocrypto: balance outside 0..2^128-1")
	ErrHash    = errors.New("nanocrypto: block hash is not 64 uppercase hex digits")
)

const alphabet = "13456789abcdefghijkmnopqrstuwxyz"

var (
	preamble   = [32]byte{31: 6} // marks a state block in the hash
	maxBalance = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))
)

// DeriveKey returns the account key for index under seed, as every Nano wallet does: Blake2b-256(seed || index).
func DeriveKey(seed [32]byte, index uint32) PrivateKey {
	return PrivateKey{[32]byte(sum(32, seed[:], binary.BigEndian.AppendUint32(nil, index)))}
}

func (k PrivateKey) Public() PublicKey {
	s, _ := k.expand()
	return PublicKey(new(edwards25519.Point).ScalarBaseMult(s).Bytes())
}

// Sign is RFC 8032 Ed25519 with Blake2b-512 in place of SHA-512. Scalar operations on the secret are constant time.
func (k PrivateKey) Sign(h Hash) Signature {
	s, prefix := k.expand()
	pub := k.Public()
	r := scalar(sum(64, prefix, h[:]))
	R := new(edwards25519.Point).ScalarBaseMult(r).Bytes()
	c := scalar(sum(64, R, pub[:], h[:]))
	var sig Signature
	copy(sig[:32], R)
	copy(sig[32:], edwards25519.NewScalar().MultiplyAdd(c, s, r).Bytes())
	return sig
}

// Verify rejects off-curve keys and non-canonical S, so a signature has exactly one valid encoding.
func Verify(pub PublicKey, h Hash, sig Signature) bool {
	A, err := new(edwards25519.Point).SetBytes(pub[:])
	if err != nil {
		return false
	}
	S, err := edwards25519.NewScalar().SetCanonicalBytes(sig[32:])
	if err != nil {
		return false
	}
	c := scalar(sum(64, sig[:32], pub[:], h[:]))
	R := new(edwards25519.Point).VarTimeDoubleScalarBaseMult(c, new(edwards25519.Point).Negate(A), S)
	return bytes.Equal(R.Bytes(), sig[:32])
}

func (p PublicKey) Address() string {
	check := sum(5, p[:])
	for i, j := 0, len(check)-1; i < j; i, j = i+1, j-1 {
		check[i], check[j] = check[j], check[i]
	}
	return "nano_" + encode(p[:], 52) + encode(check, 8)
}

// digit maps an address character to its 5-bit value; anything outside the lowercase alphabet maps to 0xff.
var digit = func() (t [256]byte) {
	for i := range t {
		t[i] = 0xff
	}
	for i := range len(alphabet) {
		t[alphabet[i]] = byte(i)
	}
	return t
}()

// ParseAddress accepts only the exact canonical form, so every string it accepts is exactly what Address
// writes for that key: lowercase alphabet only, zero padding bits, matching checksum. It runs on every click, so
// it decodes bits directly instead of through big.Int; the tests compare it against the round-trip definition.
func ParseAddress(s string) (PublicKey, error) {
	var p PublicKey
	if len(s) != 65 || s[:5] != "nano_" {
		return p, ErrAddress
	}
	// 52 characters are 260 bits: 4 zero padding bits, then the key's 256 bits, most significant first.
	// The first character therefore carries one key bit and must be '1' (0) or '3' (1).
	first := digit[s[5]]
	if first > 1 {
		return p, ErrAddress
	}
	acc, bits, out := uint32(first), 1, 0
	for i := 6; i < 57; i++ {
		v := digit[s[i]]
		if v == 0xff {
			return p, ErrAddress
		}
		acc, bits = acc<<5|uint32(v), bits+5
		if bits >= 8 {
			bits -= 8
			p[out] = byte(acc >> bits)
			out++
			acc &= 1<<bits - 1
		}
	}
	// the last 8 characters are the 40-bit checksum, Blake2b-40 of the key in reverse byte order
	check := sum(5, p[:])
	var got uint64
	for i := 57; i < 65; i++ {
		v := digit[s[i]]
		if v == 0xff {
			return PublicKey{}, ErrAddress
		}
		got = got<<5 | uint64(v)
	}
	for _, b := range check { // reversed: the hash's first byte is the checksum's least significant
		if byte(got) != b {
			return PublicKey{}, ErrAddress
		}
		got >>= 8
	}
	return p, nil
}

// Hex is the hash as Nano RPC writes it, 64 uppercase hex digits. It is not String on purpose: with a String
// method, fmt's %X would hex-encode that text instead of the bytes.
func (h Hash) Hex() string { return fmt.Sprintf("%X", h[:]) }

// ParseHash accepts only what Hex produces, so a hash stored or reported anywhere has exactly one spelling.
func ParseHash(s string) (Hash, error) {
	var h Hash
	// the length check comes first: hex.Decode panics when the input is longer than the destination
	if len(s) != 2*len(h) {
		return h, ErrHash
	}
	if _, err := hex.Decode(h[:], []byte(s)); err != nil || h.Hex() != s {
		return Hash{}, ErrHash
	}
	return h, nil
}

// StateBlock is the signed content of a Nano state block; Link is the destination key for a send.
type StateBlock struct {
	Account        PublicKey
	Previous       Hash
	Representative PublicKey
	Balance        *big.Int
	Link           [32]byte
}

func (b StateBlock) Hash() (Hash, error) {
	if b.Balance == nil || b.Balance.Sign() < 0 || b.Balance.Cmp(maxBalance) > 0 {
		return Hash{}, ErrBalance
	}
	var bal [16]byte
	b.Balance.FillBytes(bal[:])
	return Hash(sum(32, preamble[:], b.Account[:], b.Previous[:], b.Representative[:], bal[:], b.Link[:])), nil
}

// Difficulty is the value compared against the network threshold; root is the previous hash, or the account
// key for an account's first block.
func Difficulty(root [32]byte, work uint64) uint64 {
	return binary.LittleEndian.Uint64(sum(8, binary.LittleEndian.AppendUint64(nil, work), root[:]))
}

func (k PrivateKey) expand() (*edwards25519.Scalar, []byte) {
	h := sum(64, k.b[:])
	s, err := edwards25519.NewScalar().SetBytesWithClamping(h[:32])
	if err != nil {
		panic(err) // only possible for a wrong-length input, which the slice above rules out
	}
	return s, h[32:]
}

func scalar(wide []byte) *edwards25519.Scalar {
	s, err := edwards25519.NewScalar().SetUniformBytes(wide)
	if err != nil {
		panic(err) // only possible for a non-64-byte input; every caller passes sum(64, ...)
	}
	return s
}

func sum(size int, parts ...[]byte) []byte {
	h, err := blake2b.New(size, nil)
	if err != nil {
		panic(err) // sizes are constants in this file
	}
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

func encode(b []byte, chars int) string {
	n := new(big.Int).SetBytes(b)
	out := make([]byte, chars)
	for i := chars - 1; i >= 0; i-- {
		out[i] = alphabet[n.Uint64()&31]
		n.Rsh(n, 5)
	}
	return string(out)
}
