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
	adv := flag.Bool("with-progress", false, "show progress")
	del := flag.Bool("d", false, "delete duplicate files")
	by := flag.String("b", "", "compare files by hash or properties")
	flag.Parse()

	group, err := groupBy(strings.ToLower(*by))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	files, err := scanFiles(flag.Arg(0), group, *adv)
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
	fmt.Fprintf(os.Stdout, "%d files scanned - found %d duplicates", c.Uniq, c.Dupl)
	fmt.Fprintln(os.Stdout)
}

func groupBy(by string) (func(Info) Info, error) {
	var groupby func(Info) Info
	switch strings.ToLower(by) {
	case "", "hash":
		groupby = bySum
	case "name":
		groupby = byName
	default:
		return nil, fmt.Errorf("unsupported grouping value %s", by)
	}
	return groupby, nil
}

func scanFiles(dir string, groupby func(Info) Info, adv bool) (map[Info][]Info, error) {
	var quit chan int
	if adv {
		quit = make(chan int, 100)
		defer close(quit)

		go progress(quit)
	}

	sema := make(chan struct{}, 4)
	files := make(map[Info][]Info)
	err := filepath.Walk(dir, func(p string, i os.FileInfo, err error) error {
		if err != nil || i.IsDir() {
			return err
		}
		sema <- struct{}{}

		r, err := os.Open(p)
		if err != nil {
			return err
		}
		n, err := updateInfo(r, infoFromInfo(p, i))
		if err == nil {
			select {
			case quit <- int(n.Size):
			default:
			}
			k := groupby(n)
			files[k] = append(files[k], n)
		}
		return err
	})
	return files, err
}

func infoFromInfo(p string, i os.FileInfo) Info {
	return Info{
		Name: p,
		Size: i.Size(),
		Time: i.ModTime(),
	}
}

func updateInfo(r *os.File, n Info) (Info, error) {
	defer r.Close()

	digest := xxh.New64(0)
	_, err := io.Copy(digest, r)
	if err == nil {
		n.Sum = digest.Sum64()
	}
	return n, err
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

func progress(done <-chan int) {
	clear := func(n int) int {
		fmt.Fprint(os.Stdout, "\x1b[1K")
		fmt.Fprintf(os.Stdout, "\x1b[%dD", n)
		return 0
	}

	c := struct {
		Written int
		Count   int
		Size    int
		Tmp     int
	}{}
	tick := time.Tick(time.Millisecond * 625)
	for {
		var w int
		select {
		case <-tick:
			c.Tmp++
			if c.Tmp > 0 && c.Tmp%4 == 0 {
				clear(c.Written)
				c.Written, c.Tmp = 0, 0

				s := prettySize(int64(c.Size))
				w, _ = fmt.Fprintf(os.Stdout, "%d files scanned (%s)", c.Count, strings.TrimSpace(s))
			} else {
				w, _ = fmt.Fprint(os.Stdout, ".")
			}
		case n, ok := <-done:
			if !ok {
				clear(c.Written)
				return
			}
			c.Size += n
			c.Count++
		default:
		}
		c.Written += w
	}
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
	fmt.Fprintf(os.Stdout, "%s %016x  %s  %s", prefix, n.Sum, prettySize(n.Size), n.Name)
	fmt.Fprintln(os.Stdout)
}
