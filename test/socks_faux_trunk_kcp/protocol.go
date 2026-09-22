package socks_faux_kcp

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/lxt1045/rpc/test/socks_faux_trunk_kcp/pb"
)

// openHeaderLen is the fixed length prefix before a protobuf TrunkStartData.
// Format: [4B little-endian payload length][protobuf TrunkStartData][raw data]
const openHeaderLen = 4

// WriteOpenHeader writes the first message of a VirtualConn.
// It tells the server which target to dial and carries an optional first payload.
func WriteOpenHeader(w io.Writer, addr string, head []byte) error {
	msg := &pb.TrunkStartData{Addr: addr, Network: pb.Network_TCP, Body: head}
	bs, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal open header: %w", err)
	}
	if len(bs) > 1<<20 {
		return fmt.Errorf("open header too large: %d", len(bs))
	}
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], uint32(len(bs)))
	if _, err := w.Write(lb[:]); err != nil {
		return err
	}
	_, err = w.Write(bs)
	return err
}

// ReadOpenHeader reads the first message of a VirtualConn.
// It returns the target address and any head bytes included by the client.
func ReadOpenHeader(r io.Reader) (*pb.TrunkStartData, error) {
	var lb [4]byte
	if _, err := io.ReadFull(r, lb[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(lb[:])
	if n == 0 || n > 1<<20 {
		return nil, fmt.Errorf("invalid open header length %d", n)
	}
	bs := make([]byte, n)
	if _, err := io.ReadFull(r, bs); err != nil {
		return nil, err
	}
	msg := &pb.TrunkStartData{}
	if err := proto.Unmarshal(bs, msg); err != nil {
		return nil, fmt.Errorf("unmarshal open header: %w", err)
	}
	return msg, nil
}
