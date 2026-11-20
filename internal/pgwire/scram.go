package pgwire

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
)

// md5Password is the answer to AuthenticationMD5Password.
func md5Password(user, password string, salt []byte) string {
	inner := md5.Sum([]byte(password + user))
	h := hex.EncodeToString(inner[:])
	outer := md5.Sum(append([]byte(h), salt...))
	return "md5" + hex.EncodeToString(outer[:])
}

// scramClient is SCRAM-SHA-256 (RFC 5802, 7677) without channel binding.
// The user name goes in the startup message, so the exchange leaves it empty
// as libpq does.
type scramClient struct {
	password    string
	nonce       string
	firstBare   string
	serverSig   []byte
	maxIterRuns int
}

func newSCRAM(password string) *scramClient {
	b := make([]byte, 18)
	_, _ = rand.Read(b)
	return &scramClient{password: password, nonce: base64.RawStdEncoding.EncodeToString(b), maxIterRuns: 1 << 22}
}

func (s *scramClient) first() []byte {
	s.firstBare = "n=,r=" + s.nonce
	return []byte("n,," + s.firstBare)
}

var errSCRAM = errors.New("pgwire: the server's SCRAM answer couldn't be read")

func (s *scramClient) final(serverFirst []byte) ([]byte, error) {
	var nonce, salt string
	iter := 0
	for _, part := range strings.Split(string(serverFirst), ",") {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return nil, errSCRAM
		}
		switch k {
		case "r":
			nonce = v
		case "s":
			salt = v
		case "i":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, errSCRAM
			}
			iter = n
		}
	}
	if !strings.HasPrefix(nonce, s.nonce) || len(nonce) == len(s.nonce) || iter < 1 || iter > s.maxIterRuns {
		return nil, errSCRAM
	}
	rawSalt, err := base64.StdEncoding.DecodeString(salt)
	if err != nil || len(rawSalt) == 0 {
		return nil, errSCRAM
	}
	salted, err := pbkdf2.Key(sha256.New, s.password, rawSalt, iter, 32)
	if err != nil {
		return nil, errSCRAM
	}
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	withoutProof := "c=biws,r=" + nonce
	auth := s.firstBare + "," + string(serverFirst) + "," + withoutProof
	sig := mac(stored[:], auth)
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ sig[i]
	}
	s.serverSig = mac(mac(salted, "Server Key"), auth)
	return []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)), nil
}

func (s *scramClient) verify(serverFinal []byte) bool {
	v, ok := strings.CutPrefix(string(serverFinal), "v=")
	if !ok || s.serverSig == nil {
		return false
	}
	got, err := base64.StdEncoding.DecodeString(strings.TrimSpace(strings.Split(v, ",")[0]))
	return err == nil && subtle.ConstantTimeCompare(got, s.serverSig) == 1
}

func mac(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}
