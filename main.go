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

func main() {
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
		if n.Seen > 1 {
			state = KO
			c.Dupl++
		} else {
			state = OK
		}
		c.Uniq++

		line.AppendString(state, 6, linewriter.AlignCenter)
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
			r, err := os.Open(f.Name)
			if err != nil {
				return
			}
			if n, err := updateInfo(r, f); err == nil {
				seen[n.Sum]++
				n.Seen = seen[n.Sum]

				queue <- n
			}
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
				if i.IsDir() {
					return nil
				}
				queue <- infoFromInfo(p, i)
				return nil
			})
		}
	}()
	return queue
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
