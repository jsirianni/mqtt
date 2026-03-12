package server

import "errors"

// Protocol errors with reason codes for MQTT v5
var (
	ErrMalformedPacket                = errors.New("malformed packet")
	ErrProtocolError                  = errors.New("protocol error")
	ErrUnsupportedProtocolVersion     = errors.New("unsupported protocol version")
	ErrClientIDInvalid                = errors.New("client identifier not valid")
	ErrBadAuthenticationMethod        = errors.New("bad authentication method")
	ErrQoSNotSupported                = errors.New("QoS not supported")
	ErrPacketTooLarge                 = errors.New("packet too large")
	ErrTopicAliasNotSupported         = errors.New("topic alias not supported")
	ErrSubscriptionIDNotSupported     = errors.New("subscription identifier not supported")
	ErrSharedSubscriptionNotSupported = errors.New("shared subscription not supported")
	ErrFirstPacketMustBeConnect       = errors.New("first packet must be CONNECT")
	ErrInvalidUTF8                    = errors.New("invalid UTF-8")
)

// ReasonCodeFor maps common errors to MQTT reason codes
func ReasonCodeFor(err error) byte {
	if err == nil {
		return ReasonSuccess
	}
	switch {
	case errors.Is(err, ErrMalformedPacket):
		return ReasonMalformedPacket
	case errors.Is(err, ErrProtocolError):
		return ReasonProtocolError
	case errors.Is(err, ErrUnsupportedProtocolVersion):
		return ReasonUnsupportedProtocolVersion
	case errors.Is(err, ErrClientIDInvalid):
		return ReasonClientIdentifierNotValid
	case errors.Is(err, ErrBadAuthenticationMethod):
		return ReasonBadAuthenticationMethod
	case errors.Is(err, ErrQoSNotSupported):
		return ReasonQoSNotSupported
	case errors.Is(err, ErrPacketTooLarge):
		return ReasonPacketTooLarge
	case errors.Is(err, ErrTopicAliasNotSupported):
		return ReasonProtocolError
	case errors.Is(err, ErrSubscriptionIDNotSupported):
		return ReasonSubscriptionIdentifiersNotSupported
	case errors.Is(err, ErrSharedSubscriptionNotSupported):
		return ReasonWildcardSubscriptionNotSupported
	default:
		return ReasonUnspecifiedError
	}
}
