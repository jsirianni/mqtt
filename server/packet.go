package server

// Packet type nibble (high 4 bits of fixed header)
const (
	PacketCONNECT     = 1
	PacketCONNACK     = 2
	PacketPUBLISH     = 3
	PacketPUBACK      = 4
	PacketPUBREC      = 5
	PacketPUBREL      = 6
	PacketPUBCOMP     = 7
	PacketSUBSCRIBE   = 8
	PacketSUBACK      = 9
	PacketUNSUBSCRIBE = 10
	PacketUNSUBACK    = 11
	PacketPINGREQ     = 12
	PacketPINGRESP    = 13
	PacketDISCONNECT  = 14
	PacketAUTH        = 15
)

// Reason codes (common subset for v1)
const (
	ReasonSuccess                             = 0x00
	ReasonUnspecifiedError                    = 0x80
	ReasonMalformedPacket                     = 0x81
	ReasonProtocolError                       = 0x82
	ReasonUnsupportedProtocolVersion          = 0x84
	ReasonClientIdentifierNotValid            = 0x85
	ReasonNotAuthorized                       = 0x87
	ReasonBadAuthenticationMethod             = 0x8C
	ReasonReceiveMaximumExceeded              = 0x93
	ReasonPacketTooLarge                      = 0x95
	ReasonRetainNotSupported                  = 0x9A
	ReasonQoSNotSupported                     = 0x9B
	ReasonSubscriptionIdentifiersNotSupported = 0xA1
	ReasonWildcardSubscriptionNotSupported    = 0xA2
)

// Packet is the common interface for outbound packet intents.
type Packet interface {
	PacketType() byte
}

// ConnectPacket MQTT CONNECT (inbound)
type ConnectPacket struct {
	ProtocolName    string
	ProtocolVersion byte
	CleanSession    bool
	WillFlag        bool
	WillQoS         byte
	WillRetain      bool
	PasswordFlag    bool
	UsernameFlag    bool

	KeepAlive    uint16
	ConnectProps *ConnectProperties
	ClientID     string
	WillTopic    string
	WillPayload  []byte
	WillProps    *WillProperties
	Username     string
	Password     []byte
}

// PacketType returns the CONNECT packet type.
func (*ConnectPacket) PacketType() byte { return PacketCONNECT }

// MQTT 3.1.1 CONNACK return codes.
const (
	ConnackAccepted                     = 0x00
	ConnackRefusedUnacceptableProtocol  = 0x01
	ConnackRefusedIdentifierRejected    = 0x02
	ConnackRefusedServerUnavailable     = 0x03
	ConnackRefusedBadUsernameOrPassword = 0x04
	ConnackRefusedNotAuthorized         = 0x05
)

// ConnectProperties from CONNECT variable header
type ConnectProperties struct {
	SessionExpiryInterval *uint32
	ReceiveMaximum        *uint16
	MaxPacketSize         *uint32
	TopicAliasMaximum     *uint16
	RequestResponseInfo   *byte
	RequestProblemInfo    *byte
	UserProperty          [][2]string
	AuthenticationMethod  *string
	AuthenticationData    []byte
}

// WillProperties for will message
type WillProperties struct {
	WillDelayInterval      *uint32
	PayloadFormatIndicator *byte
	MessageExpiryInterval  *uint32
	ContentType            *string
	ResponseTopic          *string
	CorrelationData        []byte
	UserProperty           [][2]string
}

// ConnackPacket MQTT CONNACK (outbound)
type ConnackPacket struct {
	SessionPresent bool
	ReasonCode     byte
	Props          *ConnackProperties
}

// PacketType returns the CONNACK packet type.
func (*ConnackPacket) PacketType() byte { return PacketCONNACK }

// ConnackProperties in CONNACK
type ConnackProperties struct {
	SessionExpiryInterval *uint32
	ReceiveMaximum        *uint16
	MaxQoS                *byte
	RetainAvailable       *byte
	TopicAliasMaximum     *uint16
	WildcardSubAvailable  *byte
	SubIDAvailable        *byte
	SharedSubAvailable    *byte
	ServerKeepAlive       *uint16
	AssignedClientID      *string
	ReasonString          *string
	UserProperty          [][2]string
	MaximumPacketSize     *uint32
}

// PublishPacket MQTT PUBLISH (inbound or outbound)
type PublishPacket struct {
	Dup      bool
	QoS      byte
	Retain   bool
	Topic    string
	PacketID uint16
	Props    *PublishProperties
	Payload  []byte
}

// PacketType returns the PUBLISH packet type.
func (*PublishPacket) PacketType() byte { return PacketPUBLISH }

// PublishProperties on PUBLISH
type PublishProperties struct {
	PayloadFormatIndicator *byte
	MessageExpiryInterval  *uint32
	ContentType            *string
	ResponseTopic          *string
	CorrelationData        []byte
	UserProperty           [][2]string
	TopicAlias             *uint16
	SubscriptionID         *uint32
}

// PubackPacket MQTT PUBACK
type PubackPacket struct {
	PacketID   uint16
	ReasonCode byte
	Props      *PubackProperties
}

// PacketType returns the PUBACK packet type.
func (*PubackPacket) PacketType() byte { return PacketPUBACK }

// PubackProperties contains optional PUBACK properties.
type PubackProperties struct {
	ReasonString *string
	UserProperty [][2]string
}

// SubscribePacket MQTT SUBSCRIBE (inbound)
type SubscribePacket struct {
	PacketID uint16
	Props    *SubscribeProperties
	Filters  []SubscribeFilter
}

// PacketType returns the SUBSCRIBE packet type.
func (*SubscribePacket) PacketType() byte { return PacketSUBSCRIBE }

// SubscribeProperties contains optional SUBSCRIBE properties.
type SubscribeProperties struct {
	SubscriptionID *uint32
	UserProperty   [][2]string
}

// SubscribeFilter describes a topic filter and options for SUBSCRIBE.
type SubscribeFilter struct {
	Filter            string
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}

// SubackPacket MQTT SUBACK (outbound)
type SubackPacket struct {
	PacketID    uint16
	Props       *SubackProperties
	ReasonCodes []byte
}

// PacketType returns the SUBACK packet type.
func (*SubackPacket) PacketType() byte { return PacketSUBACK }

// SubackProperties contains optional SUBACK properties.
type SubackProperties struct {
	ReasonString *string
	UserProperty [][2]string
}

// UnsubscribePacket MQTT UNSUBSCRIBE (inbound)
type UnsubscribePacket struct {
	PacketID uint16
	Props    *UnsubscribeProperties
	Filters  []string
}

// PacketType returns the UNSUBSCRIBE packet type.
func (*UnsubscribePacket) PacketType() byte { return PacketUNSUBSCRIBE }

// UnsubscribeProperties contains optional UNSUBSCRIBE properties.
type UnsubscribeProperties struct {
	UserProperty [][2]string
}

// UnsubackPacket MQTT UNSUBACK (outbound)
type UnsubackPacket struct {
	PacketID    uint16
	Props       *UnsubackProperties
	ReasonCodes []byte
}

// PacketType returns the UNSUBACK packet type.
func (*UnsubackPacket) PacketType() byte { return PacketUNSUBACK }

// UnsubackProperties contains optional UNSUBACK properties.
type UnsubackProperties struct {
	ReasonString *string
	UserProperty [][2]string
}

// PingreqPacket MQTT PINGREQ
type PingreqPacket struct{}

// PacketType returns the PINGREQ packet type.
func (*PingreqPacket) PacketType() byte { return PacketPINGREQ }

// PingrespPacket MQTT PINGRESP
type PingrespPacket struct{}

// PacketType returns the PINGRESP packet type.
func (*PingrespPacket) PacketType() byte { return PacketPINGRESP }

// DisconnectPacket MQTT DISCONNECT (inbound or outbound)
type DisconnectPacket struct {
	ReasonCode byte
	Props      *DisconnectProperties
}

// PacketType returns the DISCONNECT packet type.
func (*DisconnectPacket) PacketType() byte { return PacketDISCONNECT }

// DisconnectProperties contains optional DISCONNECT properties.
type DisconnectProperties struct {
	SessionExpiryInterval *uint32
	ReasonString          *string
	UserProperty          [][2]string
	ServerReference       *string
}
