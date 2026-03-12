package server

import (
	"encoding/binary"
	"io"
)

// MQTT v5 property identifiers
const (
	PropPayloadFormatIndicator   = 0x01
	PropMessageExpiryInterval    = 0x02
	PropContentType              = 0x03
	PropResponseTopic            = 0x08
	PropCorrelationData          = 0x09
	PropSubscriptionIdentifier   = 0x0B
	PropSessionExpiryInterval    = 0x11
	PropAssignedClientIdentifier = 0x12
	PropServerKeepAlive          = 0x13
	PropAuthenticationMethod     = 0x15
	PropAuthenticationData       = 0x16
	PropRequestProblemInfo       = 0x17
	PropWillDelayInterval        = 0x18
	PropRequestResponseInfo      = 0x19
	PropResponseInformation      = 0x1A
	PropServerReference          = 0x1C
	PropReasonString             = 0x1F
	PropReceiveMaximum           = 0x21
	PropTopicAliasMaximum        = 0x22
	PropTopicAlias               = 0x23
	PropMaximumQoS               = 0x24
	PropRetainAvailable          = 0x25
	PropUserProperty             = 0x26
	PropMaximumPacketSize        = 0x27
	PropWildcardSubAvailable     = 0x28
	PropSubIDAvailable           = 0x29
	PropSharedSubAvailable       = 0x2A
)

// propertyReader reads properties from r until consumed. Tracks seen IDs for singleton duplicate detection.
type propertyReader struct {
	r     io.Reader
	seen  map[byte]bool
	limit int
	read  int
}

func newPropertyReader(r io.Reader, limit int) *propertyReader {
	return &propertyReader{r: r, seen: make(map[byte]bool), limit: limit}
}

func (pr *propertyReader) readByte() (byte, error) {
	if pr.read >= pr.limit {
		return 0, ErrMalformedPacket
	}
	b, err := readByte(pr.r)
	if err != nil {
		return 0, err
	}
	pr.read++
	return b, nil
}

func (pr *propertyReader) readUint16() (uint16, error) {
	if pr.read+2 > pr.limit {
		return 0, ErrMalformedPacket
	}
	v, err := readUint16(pr.r)
	if err != nil {
		return 0, err
	}
	pr.read += 2
	return v, nil
}

func (pr *propertyReader) readUint32() (uint32, error) {
	if pr.read+4 > pr.limit {
		return 0, ErrMalformedPacket
	}
	var b [4]byte
	if _, err := io.ReadFull(pr.r, b[:]); err != nil {
		return 0, err
	}
	pr.read += 4
	return binary.BigEndian.Uint32(b[:]), nil
}

func (pr *propertyReader) readUTF8() (string, error) {
	s, err := readUTF8String(pr.r)
	if err != nil {
		return "", err
	}
	pr.read += 2 + len(s)
	if pr.read > pr.limit {
		return "", ErrMalformedPacket
	}
	return s, nil
}

func (pr *propertyReader) readBinary() ([]byte, error) {
	ln, err := pr.readUint16()
	if err != nil {
		return nil, err
	}
	if pr.read+int(ln) > pr.limit {
		return nil, ErrMalformedPacket
	}
	b := make([]byte, ln)
	if _, err := io.ReadFull(pr.r, b); err != nil {
		return nil, err
	}
	pr.read += int(ln)
	return b, nil
}

func (pr *propertyReader) singleton(id byte) bool {
	if pr.seen[id] {
		return false
	}
	pr.seen[id] = true
	return true
}

// parseConnectProperties reads CONNECT variable header properties.
func parseConnectProperties(r io.Reader, propLen int) (*ConnectProperties, error) {
	if propLen == 0 {
		return nil, nil
	}
	pr := newPropertyReader(r, propLen)
	p := &ConnectProperties{}
	for pr.read < pr.limit {
		id, err := pr.readByte()
		if err != nil {
			return nil, err
		}
		switch id {
		case PropSessionExpiryInterval:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, err := pr.readUint32()
			if err != nil {
				return nil, err
			}
			p.SessionExpiryInterval = &v
		case PropReceiveMaximum:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, err := pr.readUint16()
			if err != nil {
				return nil, err
			}
			p.ReceiveMaximum = &v
		case PropMaximumPacketSize:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, err := pr.readUint32()
			if err != nil {
				return nil, err
			}
			p.MaxPacketSize = &v
		case PropTopicAliasMaximum:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, err := pr.readUint16()
			if err != nil {
				return nil, err
			}
			p.TopicAliasMaximum = &v
		case PropRequestResponseInfo:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			b, err := pr.readByte()
			if err != nil {
				return nil, err
			}
			p.RequestResponseInfo = &b
		case PropRequestProblemInfo:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			b, err := pr.readByte()
			if err != nil {
				return nil, err
			}
			p.RequestProblemInfo = &b
		case PropUserProperty:
			k, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			v, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.UserProperty = append(p.UserProperty, [2]string{k, v})
		case PropAuthenticationMethod:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			s, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.AuthenticationMethod = &s
		case PropAuthenticationData:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			b, err := pr.readBinary()
			if err != nil {
				return nil, err
			}
			p.AuthenticationData = b
		default:
			return nil, ErrMalformedPacket
		}
	}
	return p, nil
}

// parseWillProperties reads will properties (after will topic in CONNECT payload).
func parseWillProperties(r io.Reader, propLen int) (*WillProperties, error) {
	if propLen == 0 {
		return nil, nil
	}
	pr := newPropertyReader(r, propLen)
	p := &WillProperties{}
	for pr.read < pr.limit {
		id, err := pr.readByte()
		if err != nil {
			return nil, err
		}
		switch id {
		case PropWillDelayInterval:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, err := pr.readUint32()
			if err != nil {
				return nil, err
			}
			p.WillDelayInterval = &v
		case PropPayloadFormatIndicator:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			b, err := pr.readByte()
			if err != nil {
				return nil, err
			}
			p.PayloadFormatIndicator = &b
		case PropMessageExpiryInterval:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, err := pr.readUint32()
			if err != nil {
				return nil, err
			}
			p.MessageExpiryInterval = &v
		case PropContentType:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			s, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.ContentType = &s
		case PropResponseTopic:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			s, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.ResponseTopic = &s
		case PropCorrelationData:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			b, err := pr.readBinary()
			if err != nil {
				return nil, err
			}
			p.CorrelationData = b
		case PropUserProperty:
			k, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			v, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.UserProperty = append(p.UserProperty, [2]string{k, v})
		default:
			return nil, ErrMalformedPacket
		}
	}
	return p, nil
}

// encodeConnackProperties writes CONNACK properties to buf. Returns bytes written.
// Removed duplicate implementation; use appendConnackProperties.
func encodeConnackProperties(buf []byte, p *ConnackProperties) int {
	return 0
}

// We need a simpler approach: build property payload first then prepend length.
func sizeConnackProperties(p *ConnackProperties) int {
	if p == nil {
		return 0
	}
	n := 0
	if p.SessionExpiryInterval != nil {
		n += 1 + 4
	}
	if p.ReceiveMaximum != nil {
		n += 1 + 2
	}
	if p.MaxQoS != nil {
		n += 2
	}
	if p.RetainAvailable != nil {
		n += 2
	}
	if p.TopicAliasMaximum != nil {
		n += 1 + 2
	}
	if p.WildcardSubAvailable != nil {
		n += 2
	}
	if p.SubIDAvailable != nil {
		n += 2
	}
	if p.SharedSubAvailable != nil {
		n += 2
	}
	if p.ServerKeepAlive != nil {
		n += 1 + 2
	}
	if p.AssignedClientID != nil {
		n += 1 + 2 + len(*p.AssignedClientID)
	}
	if p.MaximumPacketSize != nil {
		n += 1 + 4
	}
	if n == 0 {
		return 0
	}
	return sizeVariableByteInteger(uint32(n)) + n
}

// appendConnackProperties appends CONNACK properties to buf. Returns new slice.
func appendConnackProperties(buf []byte, p *ConnackProperties) []byte {
	if p == nil {
		return buf
	}
	var payload []byte
	if p.SessionExpiryInterval != nil {
		payload = append(payload, PropSessionExpiryInterval)
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], *p.SessionExpiryInterval)
		payload = append(payload, b[:]...)
	}
	if p.ReceiveMaximum != nil {
		payload = append(payload, PropReceiveMaximum)
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], *p.ReceiveMaximum)
		payload = append(payload, b[:]...)
	}
	if p.MaxQoS != nil {
		payload = append(payload, PropMaximumQoS, *p.MaxQoS)
	}
	if p.RetainAvailable != nil {
		payload = append(payload, PropRetainAvailable, *p.RetainAvailable)
	}
	if p.TopicAliasMaximum != nil {
		payload = append(payload, PropTopicAliasMaximum)
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], *p.TopicAliasMaximum)
		payload = append(payload, b[:]...)
	}
	if p.WildcardSubAvailable != nil {
		payload = append(payload, PropWildcardSubAvailable, *p.WildcardSubAvailable)
	}
	if p.SubIDAvailable != nil {
		payload = append(payload, PropSubIDAvailable, *p.SubIDAvailable)
	}
	if p.SharedSubAvailable != nil {
		payload = append(payload, PropSharedSubAvailable, *p.SharedSubAvailable)
	}
	if p.ServerKeepAlive != nil {
		payload = append(payload, PropServerKeepAlive)
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], *p.ServerKeepAlive)
		payload = append(payload, b[:]...)
	}
	if p.AssignedClientID != nil {
		s := *p.AssignedClientID
		payload = append(payload, PropAssignedClientIdentifier)
		payload = append(payload, byte(len(s)>>8), byte(len(s)))
		payload = append(payload, s...)
	}
	if p.MaximumPacketSize != nil {
		payload = append(payload, PropMaximumPacketSize)
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], *p.MaximumPacketSize)
		payload = append(payload, b[:]...)
	}
	if len(payload) == 0 {
		return buf
	}
	vbi := make([]byte, 5)
	n := encodeVariableByteInteger(vbi, uint32(len(payload)))
	buf = append(buf, vbi[:n]...)
	buf = append(buf, payload...)
	return buf
}

// parsePublishProperties reads PUBLISH properties.
func parsePublishProperties(r io.Reader, propLen int) (*PublishProperties, error) {
	if propLen == 0 {
		return nil, nil
	}
	pr := newPropertyReader(r, propLen)
	p := &PublishProperties{}
	for pr.read < pr.limit {
		id, err := pr.readByte()
		if err != nil {
			return nil, err
		}
		switch id {
		case PropPayloadFormatIndicator:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			b, err := pr.readByte()
			if err != nil {
				return nil, err
			}
			p.PayloadFormatIndicator = &b
		case PropMessageExpiryInterval:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, err := pr.readUint32()
			if err != nil {
				return nil, err
			}
			p.MessageExpiryInterval = &v
		case PropContentType:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			s, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.ContentType = &s
		case PropResponseTopic:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			s, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.ResponseTopic = &s
		case PropCorrelationData:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			b, err := pr.readBinary()
			if err != nil {
				return nil, err
			}
			p.CorrelationData = b
		case PropUserProperty:
			k, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			v, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.UserProperty = append(p.UserProperty, [2]string{k, v})
		case PropTopicAlias:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, err := pr.readUint16()
			if err != nil {
				return nil, err
			}
			p.TopicAlias = &v
		case PropSubscriptionIdentifier:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, sz, err := readVariableByteInteger(pr.r)
			if err != nil {
				return nil, err
			}
			pr.read += sz
			if pr.read > pr.limit {
				return nil, ErrMalformedPacket
			}
			p.SubscriptionID = &v
		default:
			return nil, ErrMalformedPacket
		}
	}
	return p, nil
}

// parseSubscribeProperties reads SUBSCRIBE properties.
func parseSubscribeProperties(r io.Reader, propLen int) (*SubscribeProperties, error) {
	if propLen == 0 {
		return nil, nil
	}
	pr := newPropertyReader(r, propLen)
	p := &SubscribeProperties{}
	for pr.read < pr.limit {
		id, err := pr.readByte()
		if err != nil {
			return nil, err
		}
		switch id {
		case PropSubscriptionIdentifier:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, sz, err := readVariableByteInteger(pr.r)
			if err != nil {
				return nil, err
			}
			pr.read += sz
			if pr.read > pr.limit {
				return nil, ErrMalformedPacket
			}
			p.SubscriptionID = &v
		case PropUserProperty:
			k, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			v, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.UserProperty = append(p.UserProperty, [2]string{k, v})
		default:
			return nil, ErrMalformedPacket
		}
	}
	return p, nil
}

// parseDisconnectProperties reads DISCONNECT properties.
func parseDisconnectProperties(r io.Reader, propLen int) (*DisconnectProperties, error) {
	if propLen == 0 {
		return nil, nil
	}
	pr := newPropertyReader(r, propLen)
	p := &DisconnectProperties{}
	for pr.read < pr.limit {
		id, err := pr.readByte()
		if err != nil {
			return nil, err
		}
		switch id {
		case PropSessionExpiryInterval:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			v, err := pr.readUint32()
			if err != nil {
				return nil, err
			}
			p.SessionExpiryInterval = &v
		case PropReasonString:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			s, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.ReasonString = &s
		case PropUserProperty:
			k, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			v, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.UserProperty = append(p.UserProperty, [2]string{k, v})
		case PropServerReference:
			if !pr.singleton(id) {
				return nil, ErrMalformedPacket
			}
			s, err := pr.readUTF8()
			if err != nil {
				return nil, err
			}
			p.ServerReference = &s
		default:
			return nil, ErrMalformedPacket
		}
	}
	return p, nil
}

// appendPublishProperties appends PUBLISH properties for outbound. Nil props = empty.
func appendPublishProperties(buf []byte, p *PublishProperties) []byte {
	if p == nil {
		return buf
	}
	var payload []byte
	if p.PayloadFormatIndicator != nil {
		payload = append(payload, PropPayloadFormatIndicator, *p.PayloadFormatIndicator)
	}
	if p.MessageExpiryInterval != nil {
		payload = append(payload, PropMessageExpiryInterval)
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], *p.MessageExpiryInterval)
		payload = append(payload, b[:]...)
	}
	if p.ContentType != nil {
		payload = append(payload, PropContentType)
		payload = append(payload, byte(len(*p.ContentType)>>8), byte(len(*p.ContentType)))
		payload = append(payload, *p.ContentType...)
	}
	if p.ResponseTopic != nil {
		payload = append(payload, PropResponseTopic)
		payload = append(payload, byte(len(*p.ResponseTopic)>>8), byte(len(*p.ResponseTopic)))
		payload = append(payload, *p.ResponseTopic...)
	}
	if len(p.CorrelationData) > 0 {
		payload = append(payload, PropCorrelationData)
		payload = append(payload, byte(len(p.CorrelationData)>>8), byte(len(p.CorrelationData)))
		payload = append(payload, p.CorrelationData...)
	}
	for _, kv := range p.UserProperty {
		payload = append(payload, PropUserProperty)
		payload = append(payload, byte(len(kv[0])>>8), byte(len(kv[0])))
		payload = append(payload, kv[0]...)
		payload = append(payload, byte(len(kv[1])>>8), byte(len(kv[1])))
		payload = append(payload, kv[1]...)
	}
	if len(payload) == 0 {
		return buf
	}
	vbi := make([]byte, 5)
	n := encodeVariableByteInteger(vbi, uint32(len(payload)))
	buf = append(buf, vbi[:n]...)
	buf = append(buf, payload...)
	return buf
}

// copyPublishProps creates a copy of publish properties for forwarding (e.g. message expiry remaining).
func copyPublishProps(p *PublishProperties) *PublishProperties {
	if p == nil {
		return nil
	}
	q := &PublishProperties{}
	if p.PayloadFormatIndicator != nil {
		v := *p.PayloadFormatIndicator
		q.PayloadFormatIndicator = &v
	}
	if p.MessageExpiryInterval != nil {
		v := *p.MessageExpiryInterval
		q.MessageExpiryInterval = &v
	}
	if p.ContentType != nil {
		s := *p.ContentType
		q.ContentType = &s
	}
	if p.ResponseTopic != nil {
		s := *p.ResponseTopic
		q.ResponseTopic = &s
	}
	if len(p.CorrelationData) > 0 {
		q.CorrelationData = make([]byte, len(p.CorrelationData))
		copy(q.CorrelationData, p.CorrelationData)
	}
	q.UserProperty = make([][2]string, len(p.UserProperty))
	copy(q.UserProperty, p.UserProperty)
	return q
}
