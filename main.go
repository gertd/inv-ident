package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	dsc3 "github.com/aserto-dev/go-directory/aserto/directory/common/v3"

	"github.com/gertd/inv-ident/pkg/js"
	"github.com/gertd/inv-ident/pkg/version"

	"github.com/alecthomas/kong"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	rcOK  int = 0
	rcErr int = 1
)

func main() {
	if len(os.Args) == 1 {
		os.Args = append(os.Args, "--help")
	}

	os.Exit(run())
}

func run() (exitCode int) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cli := FlipCmd{}

	kongCtx := kong.Parse(&cli,
		kong.Name("inv-ident"),
		kong.Description("invert identities, moves the identity to be the subject of the identifier relation with the user instead of the object"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			NoAppSummary:        false,
			Summary:             false,
			Compact:             true,
			Tree:                false,
			FlagsLast:           true,
			Indenter:            kong.SpaceIndenter,
			NoExpandSubcommands: true,
		}),
		kong.Vars{},
	)

	kongCtx.BindTo(ctx, (*context.Context)(nil))

	if err := kongCtx.Run(); err != nil {
		exitErr(err)
	}

	return rcOK
}

func exitErr(err error) int {
	fmt.Fprintln(os.Stderr, err.Error())
	return rcErr
}

type FlipCmd struct {
	Input   string `flag:"" short:"i" help:"input file path" xor:"input,stdin"`
	Output  string `flag:"" short:"o" help:"output file"`
	StdIn   bool   `flag:"" name:"stdin" help:"read input from StdIn" xor:"input,stdin"`
	Version bool   `flag:"" help:"version info"`
}

func (cmd *FlipCmd) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if cmd.Version {
		fmt.Fprintln(os.Stdout, version.GetInfo().String())
		return nil
	}

	return cmd.run(ctx)
}

func (cmd *FlipCmd) run(ctx context.Context) error {
	r, err := cmd.input()
	if err != nil {
		return err
	}

	w, err := cmd.output()
	if err != nil {
		return err
	}

	return cmd.invert(ctx, r, w)
}

func (cmd *FlipCmd) input() (*os.File, error) {
	if cmd.StdIn {
		return os.Stdin, nil
	}

	fi, err := os.Stat(cmd.Input)
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		return nil, status.Errorf(codes.NotFound, "%s", cmd.Input)
	}

	return os.Open(cmd.Input)
}

func (cmd *FlipCmd) output() (*os.File, error) {
	if cmd.Output == "" {
		return os.Stdout, nil
	}

	return os.Create(cmd.Output)
}

func (cmd *FlipCmd) invert(ctx context.Context, r, w *os.File) error {
	relReader, err := js.NewReader(r)
	if err != nil {
		return err
	}
	defer relReader.Close()

	relWriter, err := js.NewWriter(w, "relations")
	if err != nil {
		return err
	}
	defer relWriter.Close()

	var msg dsc3.Relation

	for {
		err := relReader.Read(&msg)
		if err == io.EOF {
			break
		}
		if err != nil {
			if strings.Contains(err.Error(), "unknown field") {
				continue
			}
			return err
		}

		if msg.Relation == "identifier" {
			if msg.GetSubjectRelation() != "" {
				log.Fatalf("subject_relation is not empty\n%v", &msg)
			}

			objType := msg.GetObjectType()
			objID := msg.GetObjectId()
			subType := msg.GetSubjectType()
			subID := msg.GetSubjectId()

			msg.ObjectType = subType
			msg.ObjectId = subID
			msg.SubjectType = objType
			msg.SubjectId = objID
		}

		if err := relWriter.Write(&msg); err != nil {
			return err
		}
	}

	return nil
}
