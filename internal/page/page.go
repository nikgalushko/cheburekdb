package page

import (
	"encoding/json"

	"github.com/google/uuid"
)

const rowFixedPartSize = 25 // Xmin + Xmax + Mask + Ctid

type (
	Page struct {
		Header         *Header      `json:"header"`
		PointersToRows []RowPointer `json:"pointers_to_rows"`
		Rows           []Row        `json:"rows"`

		virtualID uint32
		offset    uint16
	}

	Header struct {
		CRC uint32 `json:"crc"`
	}

	RowPointer struct {
		Offset uint16 `json:"offset"`
		Size   uint16 `json:"size"`
		Mask   uint8  `json:"mask"`
	}

	Row struct {
		Xmin uint64 `json:"xmin"`
		Xmax uint64 `json:"xmax"`
		Mask uint8  `json:"mask"`
		Ctid uint64 `json:"ctid"`
		Data []byte `json:"data"`
	}
)

func Create(data []byte) (*Page, error) {
	if data == nil {
		return &Page{
			Header:    &Header{},
			virtualID: uuid.New().ID(),
		}, nil
	}

	p := &Page{}
	err := json.Unmarshal(data, p)
	if err != nil {
		return nil, err
	}

	p.virtualID = uuid.New().ID()
	p.offset = p.PointersToRows[len(p.PointersToRows)-1].Offset + p.PointersToRows[len(p.PointersToRows)-1].Size
	return p, nil
}

func (p *Page) Write(r Row) {
	p.Rows = append(p.Rows, r)
	p.PointersToRows = append(p.PointersToRows, RowPointer{
		Offset: p.offset,
		Size:   uint16(rowFixedPartSize + len(r.Data)),
	})
	p.offset += uint16(rowFixedPartSize + len(r.Data))
}

func (p *Page) Data() ([]byte, error) {
	return json.Marshal(p) // TODO: use binary format
}

func (p *Page) VirtualID() uint32 {
	return p.virtualID
}
