package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/midbel/linewriter"
	"github.com/midbel/xxh"
)

const (
	OK = "\x1b[38;5;2m[ OK ]\x1b[0m"
	KO = "\x1b[38;5;1m[ KO ]\x1b[0m"
)

type Info struct {
	Name string
	Size int64
	Sum  uint64
	Seen int
	time.Time
}

func (i *Info) Uniq() bool {
	return i.Seen <= 1
}

func (i *Info) Update() error {
	digest := xxh.New64(0)

	r, err := os.Open(i.Name)
	if err != nil {
		return err
	}
	if _, err = io.Copy(digest, r); err == nil {
		i.Sum = digest.Sum64()
	}
	return err
}

func main() {
  all := flag.Bool("a", false, "all files")
	del := flag.Bool("d", false, "delete duplicate files")
	flag.Parse()

	c := struct {
		Uniq int64
		Dupl int64
		Size int64
	}{}
	line := linewriter.NewWriter(1024, linewriter.WithPadding([]byte(" ")))
	for n := range checkFiles(scanFiles(flag.Args())) {
		var state string
		if !n.Uniq() {
			state = KO
			c.Dupl++
		} else {
			state = OK
		}
		c.Uniq++

    if !*all && n.Uniq() {
      continue
    }
    if *all {
      line.AppendString(state, 6, linewriter.AlignCenter)
    }
		line.AppendUint(n.Sum, 16, linewriter.Hex|linewriter.WithZero)
		line.AppendSize(n.Size, 7, linewriter.AlignRight)
		line.AppendString(n.Name, 0, linewriter.AlignLeft)

		if *del {
			os.Remove(n.Name)
		}
		io.Copy(os.Stdout, line)
	}
	fmt.Fprintf(os.Stdout, "%d files scanned - found %d duplicates", c.Uniq, c.Dupl)
	fmt.Fprintln(os.Stdout)
}

func checkFiles(files <-chan Info) <-chan Info {
	queue := make(chan Info)
	go func() {
		defer close(queue)

		seen := make(map[uint64]int)
		for f := range files {
			err := f.Update()
			if err != nil {
        fmt.Println("update", err)
				return
			}

			seen[f.Sum]++
			f.Seen = seen[f.Sum]

			queue <- f
		}
	}()
	return queue
}

func scanFiles(dirs []string) <-chan Info {
	queue := make(chan Info)
	go func() {
		defer close(queue)

		for i := 0; i < len(dirs); i++ {
			filepath.Walk(dirs[i], func(p string, i os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if i.IsDir() || !i.Mode().IsRegular() {
					return nil
				}
				queue <- Info{
					Name: p,
					Size: i.Size(),
					Time: i.ModTime(),
				}
				return nil
			})
		}
	}()
	return queue
}
