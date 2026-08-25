// Package delivery POSTs events to subscriber endpoints and decides when to
// give up.
package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"time"
)

// Standard Webhooks header names. Lower case because that is how the
// specification writes them; Go's http.Header canonicalises on the wire, and
// the receiving side is case-insensitive.
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
)

// Sign returns the value for the webhook-signature header.
//
// The signed content is "{id}.{timestamp}.{body}" and the result is
// "v1,<base64 HMAC-SHA256>", per https://www.standardwebhooks.com/. The version
// prefix is what lets a sender rotate secrets by presenting several signatures
// at once, space separated.
//
// services/sink verifies this, and deliberately implements the check itself
// rather than calling into here. Two implementations of one specification is
// the only arrangement where agreement means something -- a shared helper would
// sign and verify a bug consistently.
func Sign(secret []byte, id string, ts time.Time, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(id + "." + Timestamp(ts) + "."))
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// Timestamp renders the value for the webhook-timestamp header: seconds since
// the epoch. The receiver compares it against its own clock to reject replays,
// so it must be the same value that went into the signature.
func Timestamp(ts time.Time) string {
	return strconv.FormatInt(ts.Unix(), 10)
}
