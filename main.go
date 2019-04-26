package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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
	del := flag.Bool("d", false, "delete duplicate files")
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
		sort.Slice(fs, func(i, j int) bool { return fs[i].Time.Before(fs[j].Time) })
		for i, z := 0, len(fs); i < z; i++ {
			printLine(fs[i], z > 1)
			c.Size += fs[i].Size
			c.Dupl++
			if *del && z > 1 && i > 0 {
				os.Remove(fs[i].Name)
			}
		}
		c.Dupl--
		c.Uniq++
	}
	if c.Dupl < 0 {
		c.Dupl = 0
	}
	fmt.Printf("%d files scanned - found %d duplicates\n", c.Uniq, c.Dupl)
}

const (
	OK = "\x1b[38;5;2m[ OK ]\x1b[0m"
	KO = "\x1b[38;5;1m[ KO ]\x1b[0m"
)

func printLine(n Info, dup bool) {
	var prefix string
	if !dup {
		prefix = OK
	} else {
		prefix = KO
	}
	fmt.Fprintf(os.Stdout, "%s %016x  %s  %s\n", prefix, n.Sum, prettySize(n.Size), n.Name)
}

const (
	ten  = 10
	kibi = 1000
	mebi = kibi * kibi
	gibi = mebi * kibi
)

func prettySize(v int64) string {
	var (
		unit byte
		mod  int64
		div  int64
	)

	div = 1
	switch {
	default:
		mod, unit = 1, 'B'
	case v >= kibi && v < mebi:
		mod, div, unit = kibi, 10, 'K'
	case v >= mebi && v < gibi:
		mod, div, unit = mebi, 10000, 'M'
	case v >= gibi:
		mod, div, unit = gibi, 10000000, 'G'
	}
	rest := (v % mod) / div
	if rest < ten {
		rest *= ten
	}
	return fmt.Sprintf("%3d.%02d%c", v/mod, rest, unit)
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
