package main

import (
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

func (i Info) String() string {
	return i.Name
}

func main() {
	by := flag.String("b", "", "compare files by hash or properties")
	flag.Parse()

	files, err := scanFiles(flag.Arg(0), strings.ToLower(*by))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for n := range files {
		fs := files[n]
		if len(fs) == 1 {
			fmt.Printf("%s: OK\n", fs[0])
		} else {
			fmt.Printf("%s (%d)\n", fs[0], len(fs))
			for i := 1; i < len(fs); i++ {
				fmt.Printf("> %s: duplicate\n", fs[i])
			}
		}
	}
}

func scanFiles(dir, by string) (map[Info][]Info, error) {
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
		if _, err := io.Copy(digest, r); err != nil {
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
