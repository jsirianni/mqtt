package server

import (
	"strings"
	"unicode/utf8"
)

// ValidateTopicName checks a topic name for publish. No wildcards, valid UTF-8, no NUL.
func ValidateTopicName(s string) error {
	if len(s) == 0 {
		return ErrMalformedPacket
	}
	if !utf8.ValidString(s) {
		return ErrInvalidUTF8
	}
	if strings.IndexByte(s, 0) >= 0 {
		return ErrMalformedPacket
	}
	if strings.IndexAny(s, "#+") >= 0 {
		return ErrMalformedPacket
	}
	return nil
}

// ValidateTopicFilter checks a topic filter. allowShared false for v1.
func ValidateTopicFilter(s string, allowShared bool) error {
	if len(s) == 0 {
		return ErrMalformedPacket
	}
	if !utf8.ValidString(s) {
		return ErrInvalidUTF8
	}
	if strings.IndexByte(s, 0) >= 0 {
		return ErrMalformedPacket
	}
	if IsSharedFilter(s) {
		if !allowShared {
			return ErrSharedSubscriptionNotSupported
		}
		// strip $share/group/ prefix for matching; validation of the rest is same
	}
	// # must be last and alone or after /
	for i, r := range s {
		if r == '#' {
			if i+1 != len(s) {
				return ErrMalformedPacket
			}
			if i > 0 && s[i-1] != '/' {
				return ErrMalformedPacket
			}
			break
		}
	}
	// + must occupy entire level
	for i := 0; i < len(s); i++ {
		if s[i] == '+' {
			start := i
			for i < len(s) && s[i] == '+' {
				i++
			}
			if (start > 0 && s[start-1] != '/') || (i < len(s) && s[i] != '/') {
				return ErrMalformedPacket
			}
		} else {
			_, size := utf8.DecodeRuneInString(s[i:])
			i += size - 1
		}
	}
	return nil
}

// MatchTopic returns true if the topic name matches the filter. Filter may contain + and #.
// Topics starting with $ are not matched by filters that start with + or #.
func MatchTopic(filter, topic string) bool {
	if len(filter) == 0 {
		return false
	}
	if len(topic) > 0 && topic[0] == '$' {
		if len(filter) > 0 && (filter[0] == '+' || filter[0] == '#') {
			return false
		}
	}
	if filter == "#" {
		return true
	}
	return matchLevels(filter, topic)
}

// matchLevels matches filter to topic level by level. Filter may contain + and # (only at end after /).
func matchLevels(filter, topic string) bool {
	fLevels := strings.Split(filter, "/")
	tLevels := strings.Split(topic, "/")
	fi, ti := 0, 0
	for fi < len(fLevels) {
		flev := fLevels[fi]
		if flev == "#" {
			return true
		}
		if ti >= len(tLevels) {
			return false
		}
		tlev := tLevels[ti]
		if flev == "+" {
			fi++
			ti++
			continue
		}
		if flev != tlev {
			return false
		}
		fi++
		ti++
	}
	return ti == len(tLevels)
}

// IsSharedFilter returns true if filter starts with $share/
func IsSharedFilter(filter string) bool {
	return strings.HasPrefix(filter, "$share/")
}
