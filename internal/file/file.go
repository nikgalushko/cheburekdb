package file

import (
	"encoding/binary"
	"errors"
	"os"

	"github.com/nikgalushko/cheburekdb/internal/page"
)

var (
	ErrPageNotFound = errors.New("page not found")
)

type File struct {
	Header *Header
	Pages  []page.Page

	f       *os.File
	offset  int64
	offsets map[uint32]PointerToPage
	// TODO: list of dirty pages
}

type Header struct {
	CRC      uint32
	PageSize uint16
}

type PointerToPage struct {
	Offset int64
}

func New(filename string) (*File, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	header := make([]byte, 6)
	_, err = f.ReadAt(header, 0)
	if err != nil {
		return nil, err
	}

	crc := binary.LittleEndian.Uint32(header)
	pageSize := binary.LittleEndian.Uint16(header[4:])

	data := make([]byte, pageSize)
	ret := &File{
		f:       f,
		Header:  &Header{CRC: crc, PageSize: pageSize},
		offsets: make(map[uint32]PointerToPage),
		offset:  6, // crc + page size
	}
	for i := 6; i < int(info.Size()); i += int(pageSize) {
		_, err = f.ReadAt(data, int64(i))
		if err != nil {
			return nil, err
		}

		p, err := page.Create(data)
		if err != nil {
			return nil, err
		}
		ret.offsets[p.VirtualID()] = PointerToPage{Offset: int64(i)}
		ret.Pages = append(ret.Pages, *p)
	}

	return ret, nil
}

func Create(filename string, pageSize uint16) (*File, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	header := binary.LittleEndian.AppendUint32(nil, 0)
	header = binary.LittleEndian.AppendUint16(header, pageSize)
	_, err = f.WriteAt(header, 0)
	if err != nil {
		return nil, err
	}

	return &File{
		f:       f,
		Header:  &Header{PageSize: pageSize},
		offsets: make(map[uint32]PointerToPage),
		offset:  6, // crc + page size
	}, nil
}

func (f *File) AllocatePage() (*page.Page, error) {
	ptp := PointerToPage{Offset: f.offset}
	_, err := f.allocate()
	if err != nil {
		return nil, err
	}

	p, err := page.Create(nil)
	if err != nil {
		return nil, err
	}

	f.Pages = append(f.Pages, *p)
	f.offsets[p.VirtualID()] = ptp

	return p, nil
}

func (f *File) WritePage(p *page.Page) error {
	ptp, ok := f.offsets[p.VirtualID()]
	if !ok {
		return ErrPageNotFound
	}

	f.offset = ptp.Offset
	data, err := p.Data()
	if err != nil {
		return err
	}

	_, err = f.f.WriteAt(data, ptp.Offset) // TODO: check count of written bytes
	if err != nil {
		return err
	}

	return f.f.Sync()
}

func (f *File) Close() error {
	for _, p := range f.Pages {
		err := f.WritePage(&p)
		if err != nil {
			return err
		}
	}

	return f.f.Close()
}

func (f *File) allocate() ([]byte, error) {
	b := make([]byte, f.Header.PageSize)
	err := f.writeAll(b)
	if err != nil {
		return nil, err
	}

	f.offset += int64(f.Header.PageSize)
	return b, nil
}

func (f *File) writeAll(b []byte) error {
	for len(b) > 0 {
		n, err := f.f.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}
