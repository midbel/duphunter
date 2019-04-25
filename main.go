package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/midbel/xxh"
)

type Info struct {
	Name string
	Size int64
	Sum  uint64
	time.Time
}

func main() {
	by := flag.String("b", "", "compare files by hash or properties")
	flag.Parse()

	files, err := scanFiles(flag.Arg(0), strings.ToLower(*by))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	c := struct {
		Uniq int64
		Dupl int64
		Size int64
	}{}
	for n := range files {
		fs := files[n]
		if n := len(fs); n == 1 {
			printLine(fs[0], false)
			c.Size += fs[0].Size
		} else {
			for i := 0; i < n; i++ {
				printLine(fs[i], true)
				c.Dupl++
				c.Size += fs[i].Size
			}
			c.Dupl--
		}
		c.Uniq++
	}
	if c.Dupl < 0 {
		c.Dupl = 0
	}
	fmt.Printf("%d files scanned - found %d duplicates\n", c.Uniq, c.Dupl)
}

var (
	green = []byte{0x1b, '[', '3', '2', ';', '1', '0', '7', 'm'}
	red   = []byte{0x1b, '[', '3', '1', ';', '1', '0', '7', 'm'}
	reset = []byte{0x1b, '[', '0', 'm'}
)

const (
	OK = "[ OK ] "
	KO = "[ KO ] "
)

func printLine(n Info, dup bool) {
	var line bytes.Buffer
	if !dup {
		line.Write(green)
		line.WriteString(OK)
	} else {
		line.Write(red)
		line.WriteString(KO)
	}
	line.Write(reset)
	line.WriteString(fmt.Sprintf("%016x  %s\n", n.Sum, n.Name))

	io.Copy(os.Stdout, &line)
}

func progress(done <-chan int) {
	clear := func() int {
		os.Stdout.Write([]byte{0x1b, '[', '1', 'K'})
		os.Stdout.Write([]byte{0x1b, '[', '1', 'E'})
		return 0
	}
	tick := time.Tick(time.Millisecond * 750)
	defer clear()
	var (
		count int
		size  int
		tmp   int
	)
	for {
		select {
		case <-tick:
			tmp++
			if tmp > 0 && tmp%4 == 0 {
				tmp = clear()
				io.WriteString(os.Stdout, fmt.Sprintf("%d files scanned (%dMB)", count, size>>20))
			} else {
				os.Stdout.Write([]byte("."))
			}
		case n, ok := <-done:
			if !ok {
				return
			}
			size += n
			count++
		default:
		}
	}
}

func scanFiles(dir, by string) (map[Info][]Info, error) {
	quit := make(chan int, 100)
	defer close(quit)
	go progress(quit)

	var groupby func(Info) Info
	switch strings.ToLower(by) {
	case "", "hash":
		groupby = bySum
	case "name":
		groupby = byName
	default:
		return nil, fmt.Errorf("unsupported grouping value %s", by)
	}

	files := make(map[Info][]Info)
	digest := xxh.New64(0)
	buffer := make([]byte, 4<<10)

	err := filepath.Walk(dir, func(p string, i os.FileInfo, err error) error {
		if err != nil || i.IsDir() {
			return err
		}
		defer digest.Reset()

		r, err := os.Open(p)
		if err != nil {
			return err
		}
		defer r.Close()
		if _, err := io.CopyBuffer(digest, r, buffer); err != nil {
			return err
		}

		n := Info{
			Name: p,
			Size: i.Size(),
			Time: i.ModTime(),
			Sum:  digest.Sum64(),
		}
		k := groupby(n)
		files[k] = append(files[k], n)

		quit <- int(n.Size)

		return nil
	})
	return files, err
}

func byName(n Info) Info {
	n.Sum = 0
	return n
}

func bySum(n Info) Info {
	var t time.Time
	n.Name, n.Size, n.Time = "", 0, t
	return n
}
