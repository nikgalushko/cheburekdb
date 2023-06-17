package file

import (
	"path/filepath"
	"testing"

	"github.com/nikgalushko/cheburekdb/internal/page"
	"github.com/stretchr/testify/require"
)

func TestCreate(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "test.db")
	f, err := Create(filename, 1024)
	if err != nil {
		t.Fatal(err)
	}

	p1, err := f.AllocatePage()
	require.NoError(t, err)

	p2, err := f.AllocatePage()
	require.NoError(t, err)

	p2.Write(page.Row{
		Xmin: 1,
		Xmax: 0,
		Data: []byte("Hello, I'm CheburekDB!"),
	})
	p2.Write(page.Row{
		Xmin: 2,
		Xmax: 0,
		Data: []byte("Cheburek is the best food!"),
	})

	err = f.WritePage(p2)
	require.NoError(t, err)

	p1.Write(page.Row{
		Xmin: 3,
		Xmax: 0,
		Data: []byte("2+2=4"),
	})

	err = f.Close()
	require.NoError(t, err)

	f, err = New(filename)
	require.NoError(t, err)

	require.Len(t, f.Pages, 2)

	require.Equal(t, uint64(3), f.Pages[0].Rows[0].Xmin)
	require.Equal(t, uint64(0), f.Pages[0].Rows[0].Xmax)
	require.Equal(t, []byte("2+2=4"), f.Pages[0].Rows[0].Data)

	require.Equal(t, uint64(1), f.Pages[1].Rows[0].Xmin)
	require.Equal(t, uint64(0), f.Pages[1].Rows[0].Xmax)
	require.Equal(t, []byte("Hello, I'm CheburekDB!"), f.Pages[1].Rows[0].Data)

	require.Equal(t, uint64(2), f.Pages[1].Rows[1].Xmin)
	require.Equal(t, uint64(0), f.Pages[1].Rows[1].Xmax)
	require.Equal(t, []byte("Cheburek is the best food!"), f.Pages[1].Rows[1].Data)
}
