// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"bytes"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/parser/util"
)

const hexDigits = "0123456789abcdef"

// node is the struct implements Node interface except for Accept method.
// Node implementations should embed it in.
type node struct {
	utf8Text           string
	enc                charset.Encoding
	noBackslashEscapes bool
	once               *sync.Once

	text   string
	offset int

	// litRanges holds [start, end) byte offsets of string literals
	// within text (relative to start of text, not parser.src).
	// When set, Text() uses these known ranges for hex encoding
	// instead of rescanning the raw text.
	litRanges [][2]int
}

// SetOriginTextPosition implements Node interface.
func (n *node) SetOriginTextPosition(offset int) {
	n.offset = offset
}

// OriginTextPosition implements Node interface.
func (n *node) OriginTextPosition() int {
	return n.offset
}

// SetText implements Node interface.
func (n *node) SetText(enc charset.Encoding, text string) {
	n.enc = enc
	n.text = text
	n.once = &sync.Once{}
}

// SetNoBackslashEscapes marks that the SQL mode NO_BACKSLASH_ESCAPES was active
// when this node was parsed, so backslash is not treated as an escape character
// in string literals
func (n *node) SetNoBackslashEscapes(val bool) {
	if n.noBackslashEscapes == val {
		return
	}
	n.noBackslashEscapes = val
	if n.once != nil {
		n.once = &sync.Once{}
	}
}

// SetLitRanges sets the byte offset ranges of string literals within the
// node's text. Each range is [start, end) relative to the start of text.
func (n *node) SetLitRanges(ranges [][2]int) {
	n.litRanges = ranges
	if n.once != nil {
		n.once = &sync.Once{} // invalidate cache
	}
}

// Text implements Node interface.
func (n *node) Text() string {
	if n.once == nil {
		return n.text
	}
	n.once.Do(func() {
		if n.enc == nil {
			n.utf8Text = n.text
			return
		}
		if n.litRanges != nil {
			n.utf8Text = convertWithLitRanges(n.text, n.enc, n.noBackslashEscapes, n.litRanges)
		} else {
			// Fallback for external SetText() callers without lexer ranges
			n.utf8Text = convertBinaryStringLiterals(n.text, n.enc, n.noBackslashEscapes)
		}
	})
	return n.utf8Text
}

// OriginalText implements Node interface.
func (n *node) OriginalText() string {
	return n.text
}

func isPrintable(s []byte) bool {
	for len(s) > 0 {
		r, size := utf8.DecodeRune(s)
		if r == utf8.RuneError && size <= 1 {
			return false
		}
		if unicode.IsControl(r) {
			return false
		}
		s = s[size:]
	}
	return true
}

func isIdentChar(b byte) bool {
	return b == '_' ||
		(b >= '0' && b <= '9') ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z')
}

// needsSpaceBeforeHexLiteral returns true when replacing a quoted string with a
// hex literal would otherwise merge with a preceding identifier-like token
// (e.g. "_binary'...'" -> "_binary0x..."), producing invalid SQL.
func needsSpaceBeforeHexLiteral(utf8Text []byte, quoteStart int) bool {
	if quoteStart <= 0 {
		return false
	}
	return isIdentChar(utf8Text[quoteStart-1])
}

// convertWithLitRanges hex-encodes non-printable string literals using
// pre-recorded byte ranges from the lexer. Unlike convertBinaryStringLiterals,
// it does not rescan the text for quote boundaries — it operates only on the
// known litRanges, so comments and other SQL constructs are ignored entirely.
func convertWithLitRanges(text string, enc charset.Encoding, noBackslashEscapes bool, litRanges [][2]int) string {
	src := charset.HackSlice(text)

	// Transform entire text to UTF-8.
	utf8Text, _ := enc.Transform(nil, src, charset.OpDecodeReplace)

	if len(litRanges) == 0 {
		return charset.HackString(utf8Text)
	}

	// For non-UTF-8 encodings, character widths change during Transform
	// (e.g. GBK 2-byte → UTF-8 3-byte), so we need a mapping from original
	// byte offsets to UTF-8 byte offsets. For UTF-8, offsets are identical.
	isUTF8 := enc.Tp() == charset.EncodingTpUTF8 ||
		enc.Tp() == charset.EncodingTpUTF8MB3Strict ||
		enc.Tp() == charset.EncodingTpASCII
	origPos := 0
	utf8Pos := 0

	// mapOffset advances the orig→utf8 position tracker to targetOrig and
	// returns the corresponding UTF-8 position.
	mapOffset := func(targetOrig int) int {
		if isUTF8 {
			return targetOrig
		}
		for origPos < targetOrig && origPos < len(src) && utf8Pos < len(utf8Text) {
			charBytes := enc.Peek(src[origPos:])
			origStep := len(charBytes)
			if origStep == 0 {
				origStep = 1
			}
			_, utf8Step := utf8.DecodeRune(utf8Text[utf8Pos:])
			if utf8Step == 0 {
				utf8Step = 1
			}
			origPos += origStep
			utf8Pos += utf8Step
		}
		return utf8Pos
	}

	var buf *bytes.Buffer
	lastCopied := 0 // position in utf8Text up to which we've copied

	for _, lr := range litRanges {
		origStart, origEnd := lr[0], lr[1]
		if origStart < 0 || origEnd > len(src) || origStart >= origEnd {
			continue
		}

		// Quotes are at src[origStart] and src[origEnd-1].
		innerStart := origStart + 1
		innerEnd := origEnd - 1
		if innerStart >= innerEnd {
			continue // empty string ''
		}

		// Check printability of the content inside the quotes.
		decoded, err := enc.Transform(nil, src[innerStart:innerEnd], charset.OpDecode)
		if err == nil && isPrintable(decoded) {
			continue
		}

		// Non-printable: extract content (processing escape sequences) and hex-encode.
		quote := src[origStart]
		content := extractLiteralContent(src, innerStart, innerEnd, quote, noBackslashEscapes)

		utf8Start := mapOffset(origStart)
		utf8End := mapOffset(origEnd)

		if buf == nil {
			buf = &bytes.Buffer{}
			buf.Grow(len(utf8Text))
		}
		buf.Write(utf8Text[lastCopied:utf8Start])
		if needsSpaceBeforeHexLiteral(utf8Text, utf8Start) {
			buf.WriteByte(' ')
		}
		buf.WriteString("0x")
		for _, b := range content {
			buf.WriteByte(hexDigits[b>>4])
			buf.WriteByte(hexDigits[b&0xf])
		}
		lastCopied = utf8End
	}

	if buf == nil {
		return charset.HackString(utf8Text)
	}
	if lastCopied < len(utf8Text) {
		buf.Write(utf8Text[lastCopied:])
	}
	return buf.String()
}

// extractLiteralContent extracts the decoded byte content from a string
// literal in src[start:end], processing doubled-quote and backslash escapes.
func extractLiteralContent(src []byte, start, end int, quote byte, noBackslashEscapes bool) []byte {
	var content []byte
	j := start
	for j < end {
		ch := src[j]
		if ch == quote {
			j++
			if j < end && src[j] == quote {
				content = append(content, quote)
				j++
			}
		} else if ch == '\\' && !noBackslashEscapes && j+1 < end {
			j++
			content = append(content, util.UnescapeChar(src[j])...)
			j++
		} else {
			content = append(content, ch)
			j++
		}
	}
	return content
}

// convertBinaryStringLiterals processes raw SQL text, converting non-printable
// single- or double-quoted string literals to 0x hex literals and decoding
// everything else to UTF-8.
//
// The function first transforms the entire text to UTF-8 and then scans the
// UTF-8 result for string literal boundaries. This avoids ambiguity in
// encodings like GBK/GB18030 where ASCII-range bytes (e.g. 0x5C backslash)
// can appear as trail bytes of multibyte characters — UTF-8 never reuses
// ASCII byte values in multibyte sequences, so quote and backslash detection
// is always correct.
//
// A parallel index into the original byte sequence is maintained so that
// non-printable strings can be hex-encoded from their original bytes.
// Quote bytes (0x22, 0x27) are below the trail-byte range of all supported
// multibyte encodings, so finding them in the original text is always safe.
func convertBinaryStringLiterals(text string, enc charset.Encoding, noBackslashEscapes bool) string {
	src := charset.HackSlice(text)

	// Fast path: if no quotes, just transform the whole thing.
	if bytes.IndexByte(src, '\'') < 0 && bytes.IndexByte(src, '"') < 0 {
		result, _ := enc.Transform(nil, src, charset.OpDecodeReplace)
		return charset.HackString(result)
	}

	// Transform entire text to UTF-8 for safe scanning.
	utf8Text, _ := enc.Transform(nil, src, charset.OpDecodeReplace)

	var buf *bytes.Buffer
	lastCopiedIdx := 0 // tracks position in utf8Text for output assembly
	origIdx := 0       // tracks position in src (original bytes)
	i := 0             // scan position in utf8Text

	for i < len(utf8Text) {
		if utf8Text[i] != '\'' && utf8Text[i] != '"' {
			i++
			continue
		}

		utf8QuoteStart := i
		quote := utf8Text[i]
		i++

		// Find the corresponding opening quote in the original text.
		origQuoteStart := advanceOrigTo(src, &origIdx, quote)
		if origQuoteStart < 0 {
			break
		}

		// Find closing quote in UTF-8 text, keeping origIdx in sync.
		terminated := false
		var origQuoteEnd int
		for i < len(utf8Text) {
			ch := utf8Text[i]
			if ch == quote {
				i++
				origClose := advanceOrigTo(src, &origIdx, quote)
				if origClose < 0 {
					break
				}
				if i >= len(utf8Text) || utf8Text[i] != quote {
					// Closing quote.
					terminated = true
					origQuoteEnd = origClose + 1
					break
				}
				// Doubled quote escape — advance past second quote in original.
				i++
				if advanceOrigTo(src, &origIdx, quote) < 0 {
					break
				}
			} else if ch == '\\' && !noBackslashEscapes && i+1 < len(utf8Text) {
				nextCh := utf8Text[i+1]
				i += 2
				// If the escaped character is a quote byte, advance origIdx
				// past the corresponding quote in the original text so the
				// pairing stays in sync.
				if nextCh == '\'' || nextCh == '"' {
					if advanceOrigTo(src, &origIdx, nextCh) < 0 {
						break
					}
				}
			} else {
				i++
			}
		}

		if !terminated {
			continue
		}

		// Check printability by decoding the original string content.
		// OpDecode returns an error if the bytes are invalid in the source encoding;
		// isPrintable then rejects control characters in the decoded UTF-8.
		decoded, err := enc.Transform(nil, src[origQuoteStart+1:origQuoteEnd-1], charset.OpDecode)
		if err == nil && isPrintable(decoded) {
			continue
		}

		// Non-printable: extract content from original bytes and hex-encode.
		var content []byte
		j := origQuoteStart + 1
		origEnd := origQuoteEnd - 1
		for j < origEnd {
			ch := src[j]
			if ch == quote {
				j++
				if j < origEnd && src[j] == quote {
					content = append(content, quote)
					j++
				}
			} else if ch == '\\' && !noBackslashEscapes && j+1 < origEnd {
				j++
				content = append(content, util.UnescapeChar(src[j])...)
				j++
			} else {
				content = append(content, ch)
				j++
			}
		}

		// Lazy-allocate output buffer on first binary string found.
		if buf == nil {
			buf = &bytes.Buffer{}
			buf.Grow(len(utf8Text))
		}

		buf.Write(utf8Text[lastCopiedIdx:utf8QuoteStart])
		if needsSpaceBeforeHexLiteral(utf8Text, utf8QuoteStart) {
			buf.WriteByte(' ')
		}
		buf.WriteString("0x")
		for _, b := range content {
			buf.WriteByte(hexDigits[b>>4])
			buf.WriteByte(hexDigits[b&0xf])
		}
		lastCopiedIdx = i
	}

	if buf == nil {
		return charset.HackString(utf8Text)
	}

	if lastCopiedIdx < len(utf8Text) {
		buf.Write(utf8Text[lastCopiedIdx:])
	}
	return buf.String()
}

// advanceOrigTo scans src from *idx forward until it finds a byte equal to b.
// It returns the position of that byte and advances *idx past it, or returns -1
// if not found.
func advanceOrigTo(src []byte, idx *int, b byte) int {
	for *idx < len(src) {
		if src[*idx] == b {
			pos := *idx
			*idx++
			return pos
		}
		*idx++
	}
	return -1
}

// stmtNode implements StmtNode interface.
// Statement implementations should embed it in.
type stmtNode struct {
	node
}

// statement implements StmtNode interface.
func (sn *stmtNode) statement() {}

// ddlNode implements DDLNode interface.
// DDL implementations should embed it in.
type ddlNode struct {
	stmtNode
}

// ddlStatement implements DDLNode interface.
func (dn *ddlNode) ddlStatement() {}

// dmlNode is the struct implements DMLNode interface.
// DML implementations should embed it in.
type dmlNode struct {
	stmtNode
}

// dmlStatement implements DMLNode interface.
func (dn *dmlNode) dmlStatement() {}

// exprNode is the struct implements Expression interface.
// Expression implementations should embed it in.
type exprNode struct {
	node
	Type types.FieldType
	flag uint64
}

// TexprNode is exported for parser driver.
type TexprNode = exprNode

// SetType implements ExprNode interface.
func (en *exprNode) SetType(tp *types.FieldType) {
	en.Type = *tp
}

// GetType implements ExprNode interface.
func (en *exprNode) GetType() *types.FieldType {
	return &en.Type
}

// SetFlag implements ExprNode interface.
func (en *exprNode) SetFlag(flag uint64) {
	en.flag = flag
}

// GetFlag implements ExprNode interface.
func (en *exprNode) GetFlag() uint64 {
	return en.flag
}

type funcNode struct {
	exprNode
}

// functionExpression implements FunctionNode interface.
func (fn *funcNode) functionExpression() {}
