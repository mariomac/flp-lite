package flow

import (
	"bytes"

	jsonIter "github.com/json-iterator/go"
)

type RecordFields struct {
	FlowDirection   uint8
	Bytes           int
	SrcAddr         string
	DstAddr         string
	SrcMac          string
	DstMac          string
	SrcPort         uint16
	DstPort         uint16
	Etype           uint8
	Packets         int
	Proto           uint8
	TimeFlowStartMS int64
	TimeFlowEndMS   int64
	Interface       string
}
type Record struct {
	RecordFields
	// Meta holds K8s metadata. During its JSON serialization, each field here will be
	// serialized at the same level as the rest of fields, instead of as a "Meta" child object.
	Meta            map[string]interface{} // usually, 9 fields here
}

func (f *Record) Clone() Record {
	nf := *f
	nf.Meta = make(map[string]interface{}, len(f.Meta))
	for k, v := range f.Meta {
		nf.Meta[k] = v
	}
	return nf
}

var parentMarshal = jsonIter.ConfigDefault

func (f Record) MarshalJSON() ([]byte, error) {
	// shoddy way of flattening the Meta Map
	left, err := parentMarshal.Marshal(f.RecordFields)
	if err != nil {
		return nil, err
	}
	right, err := parentMarshal.Marshal(f.Meta)
	if err != nil {
		return nil, err
	}
	bb := bytes.NewBuffer(left[:len(left)-1])
	if err := bb.WriteByte(','); err != nil {
		return nil, err
	}
	if _, err := bb.Write(right[1:]); err != nil {
		return nil, err
	}
	return bb.Bytes(), nil
}


