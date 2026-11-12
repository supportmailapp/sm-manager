// Package wirejson holds the encoding/json/v2 options that keep sm-manager's
// JSON byte-compatible with what the v1 encoding/json package produced and
// accepted, so the wire contract in PROTOCOL.md does not change.
//
// v2 differs from v1 in defaults that matter here (nil slices as [], no HTML
// escaping, case-sensitive member names, null overwriting scalars, strict
// UTF-8 and duplicate-name checks). Wire undoes the ones that would change
// the frames; the golden tests in the server, status and config packages pin
// the result. Config deliberately keeps v2's strict parsing.
package wirejson

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
)

// Wire is the option set for every frame and status message, both writing and
// reading.
//
// Writing: nil slices and maps stay null, '<', '>', '&', U+2028 and U+2029 are
// escaped, and map keys are sorted, as v1 did. Raw values (jsontext.Value)
// keep their pre-escaped strings.
//
// Reading: invalid UTF-8 and lone surrogate escapes are accepted, because
// opaque business payloads carry user text that the manager only forwards and
// v1 never rejected. Member names stay case-sensitive; every client sends the
// exact camelCase names from PROTOCOL.md.
var Wire = json.JoinOptions(
	json.FormatNilSliceAsNull(true),
	json.FormatNilMapAsNull(true),
	json.Deterministic(true), // v1 sorted map keys
	jsontext.EscapeForHTML(true),
	jsontext.EscapeForJS(true),
	jsontext.PreserveRawStrings(true),
	jsontext.AllowInvalidUTF8(true),
)

// Config is the option set for config.json. It keeps v2's strict parsing so a
// bad hand-written file fails loudly at startup instead of silently falling
// back to defaults: member names are case-sensitive, repeated members and
// invalid UTF-8 are errors, and so are members the Config struct does not
// know (which is also how a wrong-case name such as "SocketPath" is caught).
// A null is rejected for every field type Config has (int, string, []string)
// instead of resetting the field to its zero value over the default.
var Config = json.JoinOptions(
	json.RejectUnknownMembers(true),
	json.WithUnmarshalers(json.JoinUnmarshalers(
		rejectNull[int]("a number"),
		rejectNull[string]("a string"),
		rejectNull[[]string]("a list of strings"),
	)),
)

// rejectNull fails on a JSON null for a value of type T. Anything else falls
// through to the default decoding. Add a line to Config when Config gains a
// field of a type not listed there.
func rejectNull[T any](what string) *json.Unmarshalers {
	return json.UnmarshalFromFunc(func(dec *jsontext.Decoder, _ *T) error {
		if dec.PeekKind() != 'n' {
			return errors.ErrUnsupported
		}
		return errors.New("null is not " + what)
	})
}
