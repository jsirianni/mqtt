package server

import (
	"bytes"
	"io"
)

const mqttProtocolName = "MQTT"
const mqttProtocolLevel = 4 // MQTT 3.1.1

func readFixedHeader(r io.Reader, maxSize uint32) (packetType byte, flags byte, remaining uint32, err error) {
	b, err := readByte(r)
	if err != nil {
		return 0, 0, 0, err
	}
	packetType = b >> 4
	flags = b & 0x0F
	remaining, _, err = decodeRemainingLength(r)
	if err != nil {
		return 0, 0, 0, err
	}
	if remaining > maxSize {
		return 0, 0, 0, ErrPacketTooLarge
	}
	return packetType, flags, remaining, nil
}

// ReadConnect reads an MQTT 3.1.1 CONNECT packet.
func ReadConnect(r io.Reader, remaining uint32) (*ConnectPacket, error) {
	limit := int(remaining)
	proto, err := readUTF8String(r)
	if err != nil {
		return nil, err
	}
	if proto != mqttProtocolName {
		return nil, ErrUnsupportedProtocolVersion
	}
	ver, err := readByte(r)
	if err != nil {
		return nil, err
	}
	if ver != mqttProtocolLevel {
		return nil, ErrUnsupportedProtocolVersion
	}
	flags, err := readByte(r)
	if err != nil {
		return nil, err
	}
	if flags&0x01 != 0 { // reserved bit must be 0
		return nil, ErrProtocolError
	}
	keepAlive, err := readUint16(r)
	if err != nil {
		return nil, err
	}

	consumed := 2 + len(proto) + 1 + 1 + 2
	clientID, err := readUTF8String(r)
	if err != nil {
		return nil, err
	}
	consumed += 2 + len(clientID)

	p := &ConnectPacket{
		ProtocolName:    proto,
		ProtocolVersion: ver,
		CleanSession:    flags&0x02 != 0,
		WillFlag:        flags&0x04 != 0,
		WillQoS:         (flags >> 3) & 0x03,
		WillRetain:      flags&0x20 != 0,
		PasswordFlag:    flags&0x40 != 0,
		UsernameFlag:    flags&0x80 != 0,
		KeepAlive:       keepAlive,
		ClientID:        clientID,
	}
	if p.WillQoS > 1 {
		return nil, ErrQoSNotSupported
	}
	if !p.WillFlag && (p.WillQoS != 0 || p.WillRetain) {
		return nil, ErrProtocolError
	}

	if p.WillFlag {
		p.WillTopic, err = readUTF8String(r)
		if err != nil {
			return nil, err
		}
		consumed += 2 + len(p.WillTopic)
		willPayloadLen, err := readUint16(r)
		if err != nil {
			return nil, err
		}
		consumed += 2
		if willPayloadLen > 0 {
			p.WillPayload = make([]byte, willPayloadLen)
			if _, err := io.ReadFull(r, p.WillPayload); err != nil {
				return nil, err
			}
			consumed += int(willPayloadLen)
		}
	}
	if p.UsernameFlag {
		p.Username, err = readUTF8String(r)
		if err != nil {
			return nil, err
		}
		consumed += 2 + len(p.Username)
	}
	if p.PasswordFlag {
		ln, err := readUint16(r)
		if err != nil {
			return nil, err
		}
		consumed += 2
		if ln > 0 {
			p.Password = make([]byte, ln)
			if _, err := io.ReadFull(r, p.Password); err != nil {
				return nil, err
			}
			consumed += int(ln)
		}
	}
	if consumed != limit {
		return nil, ErrMalformedPacket
	}
	return p, nil
}

// WriteConnack serializes MQTT 3.1.1 CONNACK.
func WriteConnack(w io.Writer, p *ConnackPacket) error {
	sp := byte(0)
	if p.SessionPresent {
		sp = 1
	}
	buf := []byte{
		PacketCONNACK << 4,
		2, // remaining length
		sp,
		p.ReasonCode,
	}
	_, err := w.Write(buf)
	return err
}

func ReadPublish(r io.Reader, remaining uint32, flags byte) (*PublishPacket, error) {
	dup := (flags & 0x08) != 0
	qos := (flags >> 1) & 0x03
	retain := (flags & 1) != 0
	if qos > 1 {
		return nil, ErrQoSNotSupported
	}
	topic, err := readUTF8String(r)
	if err != nil {
		return nil, err
	}
	var packetID uint16
	if qos > 0 {
		packetID, err = readUint16(r)
		if err != nil {
			return nil, err
		}
		if packetID == 0 {
			return nil, ErrMalformedPacket
		}
	}
	payloadLen := int(remaining) - (2 + len(topic))
	if qos > 0 {
		payloadLen -= 2
	}
	if payloadLen < 0 {
		return nil, ErrMalformedPacket
	}
	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}
	return &PublishPacket{
		Dup:      dup,
		QoS:      qos,
		Retain:   retain,
		Topic:    topic,
		PacketID: packetID,
		Payload:  payload,
	}, nil
}

func WritePublish(w io.Writer, p *PublishPacket, maxPacketSize uint32) error {
	if maxPacketSize == 0 {
		maxPacketSize = 268435455
	}
	headerLen := 2 + len(p.Topic)
	if p.QoS > 0 {
		headerLen += 2
	}
	payloadLen := len(p.Payload)
	total := 1 + sizeRemainingLength(uint32(headerLen+payloadLen)) + headerLen + payloadLen
	if total > int(maxPacketSize) {
		return ErrPacketTooLarge
	}
	flags := byte(0)
	if p.Dup {
		flags |= 0x08
	}
	flags |= (p.QoS << 1)
	if p.Retain {
		flags |= 0x01
	}
	buf := make([]byte, 0, total)
	buf = append(buf, PacketPUBLISH<<4|flags)
	rlBuf := make([]byte, 5)
	n := encodeRemainingLength(rlBuf, uint32(headerLen+payloadLen))
	buf = append(buf, rlBuf[:n]...)
	buf = append(buf, byte(len(p.Topic)>>8), byte(len(p.Topic)))
	buf = append(buf, p.Topic...)
	if p.QoS > 0 {
		buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	}
	buf = append(buf, p.Payload...)
	_, err := w.Write(buf)
	return err
}

func ReadPuback(r io.Reader, remaining uint32) (*PubackPacket, error) {
	if remaining != 2 {
		return nil, ErrMalformedPacket
	}
	packetID, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	return &PubackPacket{PacketID: packetID, ReasonCode: ReasonSuccess}, nil
}

func WritePuback(w io.Writer, p *PubackPacket) error {
	buf := []byte{
		PacketPUBACK << 4,
		2,
		byte(p.PacketID >> 8), byte(p.PacketID),
	}
	_, err := w.Write(buf)
	return err
}

func ReadSubscribe(r io.Reader, remaining uint32) (*SubscribePacket, error) {
	packetID, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	consumed := 2
	var filters []SubscribeFilter
	for consumed < int(remaining) {
		filter, err := readUTF8String(r)
		if err != nil {
			return nil, err
		}
		consumed += 2 + len(filter)
		opt, err := readByte(r)
		if err != nil {
			return nil, err
		}
		consumed++
		filters = append(filters, SubscribeFilter{
			Filter: filter,
			QoS:    opt & 0x03,
		})
	}
	return &SubscribePacket{PacketID: packetID, Filters: filters}, nil
}

func WriteSuback(w io.Writer, p *SubackPacket) error {
	reasonCodes := make([]byte, len(p.ReasonCodes))
	for i, rc := range p.ReasonCodes {
		// MQTT 3.1.1 SUBACK supports only 0x00,0x01,0x02,0x80
		if rc <= 2 || rc == 0x80 {
			reasonCodes[i] = rc
			continue
		}
		reasonCodes[i] = 0x80
	}
	bodyLen := 2 + len(reasonCodes)
	buf := make([]byte, 0, 2+5+bodyLen)
	buf = append(buf, PacketSUBACK<<4)
	rlBuf := make([]byte, 5)
	n := encodeRemainingLength(rlBuf, uint32(bodyLen))
	buf = append(buf, rlBuf[:n]...)
	buf = append(buf, byte(p.PacketID>>8), byte(p.PacketID))
	buf = append(buf, reasonCodes...)
	_, err := w.Write(buf)
	return err
}

func ReadUnsubscribe(r io.Reader, remaining uint32) (*UnsubscribePacket, error) {
	packetID, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	consumed := 2
	var filters []string
	for consumed < int(remaining) {
		f, err := readUTF8String(r)
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
		consumed += 2 + len(f)
	}
	return &UnsubscribePacket{PacketID: packetID, Filters: filters}, nil
}

func WriteUnsuback(w io.Writer, p *UnsubackPacket) error {
	buf := []byte{
		PacketUNSUBACK << 4,
		2,
		byte(p.PacketID >> 8), byte(p.PacketID),
	}
	_, err := w.Write(buf)
	return err
}

func WritePingresp(w io.Writer) error {
	_, err := w.Write([]byte{PacketPINGRESP << 4, 0})
	return err
}

func ReadDisconnect(r io.Reader, remaining uint32) (*DisconnectPacket, error) {
	if remaining != 0 {
		return nil, ErrMalformedPacket
	}
	return &DisconnectPacket{ReasonCode: ReasonSuccess}, nil
}

func WriteDisconnect(w io.Writer, _ *DisconnectPacket) error {
	_, err := w.Write([]byte{PacketDISCONNECT << 4, 0})
	return err
}

// ReadPacket reads one MQTT packet from r. maxPacketSize caps remaining length.
func ReadPacket(r io.Reader, maxPacketSize uint32) (interface{}, uint32, error) {
	pt, flags, remaining, err := readFixedHeader(r, maxPacketSize)
	if err != nil {
		return nil, 0, err
	}
	// Fixed header flag checks required by MQTT.
	switch pt {
	case PacketCONNECT, PacketCONNACK, PacketSUBACK, PacketUNSUBACK, PacketPINGREQ, PacketPINGRESP, PacketDISCONNECT:
		if flags != 0 {
			return nil, 0, ErrMalformedPacket
		}
	case PacketSUBSCRIBE, PacketUNSUBSCRIBE:
		if flags != 0x02 {
			return nil, 0, ErrMalformedPacket
		}
	}

	totalLen := remaining
	rest := int(remaining)
	if rest == 0 {
		switch pt {
		case PacketPINGREQ:
			return &PingreqPacket{}, totalLen, nil
		case PacketDISCONNECT:
			return &DisconnectPacket{ReasonCode: ReasonSuccess}, totalLen, nil
		default:
			return nil, 0, ErrMalformedPacket
		}
	}
	payload := make([]byte, rest)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	br := bytes.NewReader(payload)
	switch pt {
	case PacketCONNECT:
		p, err := ReadConnect(br, remaining)
		return p, totalLen, err
	case PacketPUBLISH:
		p, err := ReadPublish(br, remaining, flags)
		return p, totalLen, err
	case PacketPUBACK:
		p, err := ReadPuback(br, remaining)
		return p, totalLen, err
	case PacketSUBSCRIBE:
		p, err := ReadSubscribe(br, remaining)
		return p, totalLen, err
	case PacketUNSUBSCRIBE:
		p, err := ReadUnsubscribe(br, remaining)
		return p, totalLen, err
	case PacketDISCONNECT:
		p, err := ReadDisconnect(br, remaining)
		return p, totalLen, err
	default:
		return nil, 0, ErrMalformedPacket
	}
}

func WritePacket(w io.Writer, pkt Packet, maxPacketSize uint32) error {
	if maxPacketSize == 0 {
		maxPacketSize = 268435455
	}
	switch p := pkt.(type) {
	case *ConnackPacket:
		return WriteConnack(w, p)
	case *PublishPacket:
		return WritePublish(w, p, maxPacketSize)
	case *PubackPacket:
		return WritePuback(w, p)
	case *SubackPacket:
		return WriteSuback(w, p)
	case *UnsubackPacket:
		return WriteUnsuback(w, p)
	case *PingrespPacket:
		return WritePingresp(w)
	case *DisconnectPacket:
		return WriteDisconnect(w, p)
	default:
		return ErrProtocolError
	}
}
