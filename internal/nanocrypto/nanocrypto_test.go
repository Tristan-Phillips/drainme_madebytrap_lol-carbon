package nanocrypto

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
)

type vectors struct {
	Keys []struct {
		Seed, Private, Public, Address string
		Index                          uint32
	}
	Blocks []struct {
		Account, Previous, Representative, Balance, Link, Hash, Signature string
	}
	Work []struct{ Root, Work, Difficulty string }
}

type chainBlock struct {
	Hash, Subtype, Account, Previous, Representative, Balance, Link, Signature, Work string
}

func load(t *testing.T, name string, v any) {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatal(err)
	}
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustAddr(t *testing.T, s string) PublicKey {
	t.Helper()
	p, err := ParseAddress(s)
	if err != nil {
		t.Fatalf("%s: %v", s, err)
	}
	return p
}

func balance(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("bad balance %q", s)
	}
	return n
}

func TestKeyVectors(t *testing.T) {
	var v vectors
	load(t, "vectors.json", &v)
	for _, k := range v.Keys {
		key := DeriveKey([32]byte(unhex(t, k.Seed)), k.Index)
		if fmt.Sprintf("%X", key.b) != k.Private {
			t.Errorf("seed %s/%d: private key mismatch", k.Seed, k.Index)
		}
		pub := key.Public()
		if fmt.Sprintf("%X", pub) != k.Public {
			t.Errorf("seed %s/%d: public %X, want %s", k.Seed, k.Index, pub, k.Public)
		}
		if pub.Address() != k.Address {
			t.Errorf("address %s, want %s", pub.Address(), k.Address)
		}
		if mustAddr(t, k.Address) != pub {
			t.Errorf("ParseAddress(%s) does not round-trip", k.Address)
		}
	}
}

func TestBlockVectors(t *testing.T) {
	var v vectors
	load(t, "vectors.json", &v)
	signer := DeriveKey([32]byte{}, 0) // the generator signed every block with the all-zero seed, index 0
	for _, b := range v.Blocks {
		blk := StateBlock{
			Account:        mustAddr(t, b.Account),
			Previous:       Hash(unhex(t, b.Previous)),
			Representative: mustAddr(t, b.Representative),
			Balance:        balance(t, b.Balance),
			Link:           [32]byte(unhex(t, b.Link)),
		}
		h, err := blk.Hash()
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%X", h) != b.Hash {
			t.Errorf("balance %s: hash %X, want %s", b.Balance, h, b.Hash)
		}
		sig := signer.Sign(h)
		if fmt.Sprintf("%X", sig) != b.Signature {
			t.Errorf("balance %s: signature mismatch", b.Balance)
		}
		if !Verify(blk.Account, h, sig) {
			t.Errorf("balance %s: own signature does not verify", b.Balance)
		}
	}
}

// real mainnet blocks: the network accepted these, so our hash, verify and difficulty must agree with it
func TestChainBlocks(t *testing.T) {
	var blocks []chainBlock
	load(t, "chain_blocks.json", &blocks)
	if len(blocks) == 0 {
		t.Fatal("no chain blocks loaded")
	}
	threshold := map[string]uint64{"send": 0xfffffff800000000, "receive": 0xfffffe0000000000}
	for _, b := range blocks {
		blk := StateBlock{
			Account:        mustAddr(t, b.Account),
			Previous:       Hash(unhex(t, b.Previous)),
			Representative: mustAddr(t, b.Representative),
			Balance:        balance(t, b.Balance),
			Link:           [32]byte(unhex(t, b.Link)),
		}
		h, err := blk.Hash()
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%X", h) != b.Hash {
			t.Errorf("%s: hash %X, want %s", b.Subtype, h, b.Hash)
		}
		if !Verify(blk.Account, h, Signature(unhex(t, b.Signature))) {
			t.Errorf("%s %s: real signature rejected", b.Subtype, b.Hash)
		}
		root := [32]byte(blk.Previous)
		if blk.Previous == (Hash{}) {
			root = blk.Account
		}
		work, _ := new(big.Int).SetString(b.Work, 16)
		if Difficulty(root, work.Uint64()) < threshold[b.Subtype] {
			t.Errorf("%s %s: real work below threshold", b.Subtype, b.Hash)
		}
	}
}

func TestWorkVectors(t *testing.T) {
	var v vectors
	load(t, "vectors.json", &v)
	for _, w := range v.Work {
		work, _ := new(big.Int).SetString(w.Work, 16)
		got := fmt.Sprintf("%016x", Difficulty([32]byte(unhex(t, w.Root)), work.Uint64()))
		if got != w.Difficulty {
			t.Errorf("work %s: difficulty %s, want %s", w.Work, got, w.Difficulty)
		}
	}
}

func TestVerifyRejectsTampering(t *testing.T) {
	key := DeriveKey([32]byte{1}, 0)
	pub := key.Public()
	h := Hash{42}
	sig := key.Sign(h)

	other := DeriveKey([32]byte{2}, 0).Public()
	badHash := h
	badHash[0] ^= 1
	badR := sig
	badR[0] ^= 1
	badS := sig
	badS[40] ^= 1
	nonCanonicalS := sig // S + l still satisfies the equation unless canonical encoding is enforced
	l, _ := new(big.Int).SetString("7237005577332262213973186563042994240857116359379907606001950938285454250989", 10)
	sBig := new(big.Int).SetBytes(reverse(sig[32:]))
	copy(nonCanonicalS[32:], reverse(sBig.Add(sBig, l).FillBytes(make([]byte, 32))))
	offCurve := PublicKey{2} // y=2 has no point on the curve

	cases := map[string]bool{
		"valid":           Verify(pub, h, sig),
		"wrong key":       Verify(other, h, sig),
		"wrong hash":      Verify(pub, badHash, sig),
		"tampered R":      Verify(pub, h, badR),
		"tampered S":      Verify(pub, h, badS),
		"non-canonical S": Verify(pub, h, nonCanonicalS),
		"off-curve key":   Verify(offCurve, h, sig),
		"zero signature":  Verify(pub, h, Signature{}),
	}
	for name, ok := range cases {
		if ok != (name == "valid") {
			t.Errorf("%s: Verify = %v", name, ok)
		}
	}
}

func reverse(b []byte) []byte {
	out := make([]byte, len(b))
	for i := range b {
		out[len(b)-1-i] = b[i]
	}
	return out
}

func TestParseAddressRejects(t *testing.T) {
	good := DeriveKey([32]byte{}, 0).Public().Address()
	flip := func(i int, c byte) string { b := []byte(good); b[i] = c; return string(b) }
	// swap a character for a different one that is still in the alphabet
	other := func(i int) string {
		if good[i] == '1' {
			return flip(i, '3')
		}
		return flip(i, '1')
	}
	cases := map[string]string{
		"empty":            "",
		"legacy xrb_":      "xrb_" + good[5:],
		"wrong prefix":     "nanx_" + good[5:],
		"uppercase":        strings.ToUpper(good),
		"too short":        good[:64],
		"too long":         good + "1",
		"trailing space":   good[:64] + " ",
		"leading space":    " " + good[1:],
		"bad checksum":     other(64),
		"key changed":      other(30),
		"char 0":           flip(20, '0'),
		"char 2":           flip(20, '2'),
		"char l":           flip(20, 'l'),
		"char v":           flip(20, 'v'),
		"non-ascii":        good[:63] + "é",
		"key over 256 bit": flip(5, '9'),
	}
	for name, s := range cases {
		if _, err := ParseAddress(s); !errors.Is(err, ErrAddress) {
			t.Errorf("%s (%q): want ErrAddress, got %v", name, s, err)
		}
	}
}

// parseAddressReference is the definition ParseAddress must match: decode through big.Int, then accept only if
// re-encoding gives back the identical string. It was the implementation until the fast decoder replaced it.
func parseAddressReference(s string) (PublicKey, error) {
	var p PublicKey
	if len(s) != 65 || !strings.HasPrefix(s, "nano_") {
		return p, ErrAddress
	}
	n := new(big.Int)
	for i := 5; i < 57; i++ {
		v := strings.IndexByte(alphabet, s[i])
		if v < 0 {
			return p, ErrAddress
		}
		n.Lsh(n, 5).Or(n, big.NewInt(int64(v)))
	}
	if n.BitLen() > 256 {
		return p, ErrAddress
	}
	n.FillBytes(p[:])
	if p.Address() != s {
		return p, ErrAddress
	}
	return p, nil
}

func sameParse(t *testing.T, s string) {
	t.Helper()
	p, err := ParseAddress(s)
	q, errRef := parseAddressReference(s)
	if (err == nil) != (errRef == nil) || (err == nil && p != q) {
		t.Fatalf("%q: fast %X %v, reference %X %v", s, p, err, q, errRef)
	}
}

// every one-character change to 50 valid addresses, each against the reference: the near misses are where a
// decoder goes wrong, and random fuzzing almost never produces a valid checksum to start from
func TestParseAddressMatchesReference(t *testing.T) {
	chars := alphabet + "02lvxyzABNOX_ \x00\xff"
	for k := 0; k < 50; k++ {
		good := DeriveKey([32]byte{byte(k), 0x3a}, 0).Public().Address()
		sameParse(t, good)
		for i := 0; i < len(good); i++ {
			for j := 0; j < len(chars); j++ {
				sameParse(t, good[:i]+chars[j:j+1]+good[i+1:])
			}
		}
		sameParse(t, good[:64])
		sameParse(t, good+"1")
	}
	// keys at the edges of the padding bits
	for _, p := range []PublicKey{{}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, {0x80}, {31: 1}} {
		sameParse(t, p.Address())
		sameParse(t, "nano_3"+p.Address()[6:]) // sets a padding bit when the first character was '1'
		sameParse(t, "nano_5"+p.Address()[6:])
	}
}

func FuzzParseAddress(f *testing.F) {
	f.Add(DeriveKey([32]byte{}, 0).Public().Address())
	f.Add("nano_")
	f.Add("xrb_3i1aq1cchnmbn9x5rsbap8b15akfh7wj7pwskuzi7ahz8oq6cobd99d4r3b7")
	f.Fuzz(func(t *testing.T, s string) {
		sameParse(t, s)
		p, err := ParseAddress(s)
		if err == nil && p.Address() != s {
			t.Fatalf("accepted non-canonical %q", s)
		}
	})
}

// any 32 bytes round-trip through Address and back, through both decoders
func FuzzAddressRoundTrip(f *testing.F) {
	f.Add(make([]byte, 32))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) != 32 {
			return
		}
		a := PublicKey(b).Address()
		sameParse(t, a)
		if p, err := ParseAddress(a); err != nil || p != PublicKey(b) {
			t.Fatalf("%X: %X %v", b, p, err)
		}
	})
}

func TestHashRejectsBalance(t *testing.T) {
	over := new(big.Int).Add(maxBalance, big.NewInt(1))
	for name, bal := range map[string]*big.Int{"nil": nil, "negative": big.NewInt(-1), "2^128": over} {
		if _, err := (StateBlock{Balance: bal}).Hash(); !errors.Is(err, ErrBalance) {
			t.Errorf("%s: want ErrBalance, got %v", name, err)
		}
	}
	for name, bal := range map[string]*big.Int{"zero": big.NewInt(0), "max": maxBalance} {
		if _, err := (StateBlock{Balance: bal}).Hash(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestPrivateKeyNeverPrints(t *testing.T) {
	key := DeriveKey([32]byte{}, 0)
	secret := strings.ToLower(fmt.Sprintf("%x", key.b))
	wrapped := struct{ K PrivateKey }{key}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%x", "%X", "%d", "%q"} {
		for _, val := range []any{key, &key, wrapped} {
			out := strings.ToLower(fmt.Sprintf(verb, val))
			if strings.Contains(out, secret[:16]) || !strings.Contains(out, "redacted") {
				t.Errorf("%s of %T printed %q", verb, val, out)
			}
		}
	}
	if b, _ := json.Marshal(wrapped); strings.Contains(strings.ToLower(string(b)), secret[:16]) {
		t.Errorf("json leaked key: %s", b)
	}
}

func TestParseHash(t *testing.T) {
	h := Hash{0x01, 0xAB, 31: 0xFF}
	if got, err := ParseHash(h.Hex()); err != nil || got != h {
		t.Fatalf("round trip: %X %v", got, err)
	}
	if h.Hex() != "01AB0000000000000000000000000000000000000000000000000000000000FF" {
		t.Fatalf("Hex = %s", h.Hex())
	}
	good := h.Hex()
	for _, s := range []string{"", strings.ToLower(good), good[:62], good + "00", "G" + good[1:], " " + good[1:], good[:63] + "\n"} {
		if _, err := ParseHash(s); !errors.Is(err, ErrHash) {
			t.Errorf("%q accepted", s)
		}
	}
}
