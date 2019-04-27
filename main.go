package main

import (
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/midbel/cli"
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

	time.Time

	Sim uint32
	Sum uint64

	Seen int
}

func (i Info) Distance(n Info) float64 {
	var score float64

	curr, other := i.Sim, n.Sim
	for j := 0; j < 32; j++ {
		c, o := curr&0x1, other&0x1
		if c != o {
			score++
		}
		curr, other = curr>>1, other>>1
	}
	return 1 - (score / 32)
}

func (i *Info) Uniq() bool {
	return i.Seen <= 1
}

func (i *Info) Update() error {
	digest := xxh.New64(0)
	sim := Simhash()

	r, err := os.Open(i.Name)
	if err != nil {
		return err
	}
	if _, err = io.Copy(io.MultiWriter(sim, digest), r); err == nil {
		i.Sum = digest.Sum64()
		i.Sim = sim.Sum32()
	}
	return err
}

var commands = []*cli.Command{
	{
		Usage: "list <dir...>",
		Short: "",
		Run:   runList,
	},
	{
		Usage: "similar <file> <other...>",
		Short: "",
		Run:   runSim,
	},
	{
		Usage: "duplicate <file> <other...>",
		Short: "",
		Run:   runDup,
	},
}

const helpText = `{{.Name}} scan the HRDP archive to consolidate the USOC HRDP archive

Usage:

  {{.Name}} command [options] <arguments>

Available commands:

{{range .Commands}}{{if .Runnable}}{{printf "  %-12s %s" .String .Short}}{{if .Alias}} (alias: {{ join .Alias ", "}}){{end}}{{end}}
{{end}}
Use {{.Name}} [command] -h for more information about its usage.
`

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "unexpected error: %s\n", err)
		}
	}()
	if err := cli.Run(commands, cli.Usage("tmcat", helpText, commands), nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(6)
	}
}

func Line() *linewriter.Writer {
	return linewriter.NewWriter(1024, linewriter.WithPadding([]byte(" ")))
}

func runList(cmd *cli.Command, args []string) error {
	all := cmd.Flag.Bool("a", false, "show all files")
	del := cmd.Flag.Bool("d", false, "delete duplicate files")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}

	c := struct {
		Uniq int64
		Dupl int64
		Size int64
	}{}
	line := Line()
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

	return nil
}

func runDup(cmd *cli.Command, args []string) error {
	all := cmd.Flag.Bool("a", false, "show all files")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	x, err := infoFromPath(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	if err := x.Update(); err != nil {
		return err
	}
	line := Line()
	for j := 1; j < cmd.Flag.NArg(); j++ {
		n, err := infoFromPath(cmd.Flag.Arg(j))
		if err != nil {
			continue
		}
		if err := n.Update(); err != nil {
			continue
		}
		if !*all && (x.Sum != n.Sum || x.Name == n.Name) {
			continue
		}
		if *all {
			var state string
			if x.Sum == n.Sum {
				state = KO
			} else {
				state = OK
			}
			line.AppendString(state, 6, linewriter.AlignCenter)
		}
		line.AppendUint(n.Sum, 16, linewriter.Hex|linewriter.WithZero)
		line.AppendSize(n.Size, 7, linewriter.AlignRight)
		line.AppendString(n.Name, 0, linewriter.AlignLeft)

		io.Copy(os.Stdout, line)
	}
	return nil
}

func runSim(cmd *cli.Command, args []string) error {
	threshold := cmd.Flag.Float64("p", 0.0, "threshold")
	if err := cmd.Flag.Parse(args); err != nil {
		return err
	}
	n, err := infoFromPath(cmd.Flag.Arg(0))
	if err != nil {
		return err
	}
	if err := n.Update(); err != nil {
		return err
	}
	line := Line()
	for j := 1; j < cmd.Flag.NArg(); j++ {
		x, err := infoFromPath(cmd.Flag.Arg(j))
		if err != nil {
			continue
		}
		if err := x.Update(); err != nil {
			continue
		}
		dist := n.Distance(x)
		if *threshold > 0 {
			var state string
			if d := dist * 100; d >= *threshold {
				state = OK
			} else {
				state = KO
			}
			line.AppendString(state, 6, linewriter.AlignCenter)
		}
		line.AppendUint(uint64(x.Sim), 8, linewriter.WithZero|linewriter.Hex)
		line.AppendPercent(dist, 7, 2, linewriter.AlignRight)
		line.AppendString(x.Name, 0, linewriter.AlignLeft)

		io.Copy(os.Stdout, line)
	}
	return nil
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
				queue <- infoFromInfo(p, i)
				return nil
			})
		}
	}()
	return queue
}

func infoFromPath(p string) (Info, error) {
	var n Info
	i, err := os.Stat(p)
	if err == nil {
		if i.Mode().IsRegular() {
			n = infoFromInfo(p, i)
		} else {
			err = fmt.Errorf("%s not a regular file", p)
		}
	}
	return n, err
}

func infoFromInfo(p string, i os.FileInfo) Info {
	return Info{
		Name: p,
		Size: i.Size(),
		Time: i.ModTime(),
	}
}

type simhash struct {
	calculate func([]byte) uint32
	state     []int
}

func Simhash() hash.Hash32 {
	return &simhash{
		calculate: djb2,
		state:     make([]int, 32),
	}
}

func (s *simhash) Write(bs []byte) (int, error) {
	hs := s.calculate(bs)
	for i := 0; i < len(s.state); i++ {
		if bit := hs & 0x1; bit == 1 {
			s.state[i]++
		} else {
			s.state[i]--
		}
		hs = hs >> 1
	}
	return len(bs), nil
}

func djb2(bs []byte) uint32 {
	hs := uint32(5381)
	for i := 0; i < len(bs); i++ {
		hs = hs*33 + uint32(bs[i])
	}
	return hs
}

func (s *simhash) Reset() {
	s.state = make([]int, 0)
}

func (s *simhash) Sum(bs []byte) []byte {
	if len(bs) < 4 {
		bs = make([]byte, 4)
	}
	return bs
}

func (s *simhash) Sum32() uint32 {
	defer s.Reset()

	var state uint32
	for i := len(s.state) - 1; i >= 0; i-- {
		if s.state[i] > 0 {
			state |= 1
		}
		state = state << 1
	}
	return state
}

func (s *simhash) BlockSize() int { return 4096 }
func (s *simhash) Size() int      { return 4 }
