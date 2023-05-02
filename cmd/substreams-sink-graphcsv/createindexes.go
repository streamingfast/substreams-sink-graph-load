package main

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
)

var createIndexesCmd = Command(createIndexesE,
	"create-indexes <entity_name>",
	`Receive all of the create DDLs from 'graphman index generate-ddl' from 'stdin'
    and output the 'create index if not exists' statements for the provided <entity_name>.
	`,
	MaximumNArgs(1),
	Flags(func(flags *pflag.FlagSet) {
		flags.Bool("skip-constraint", false, "")
		flags.Bool("drop", false, "Drop instead of creates")
	}),
)

var createTableRegexp = regexp.MustCompile(`(?m)\s*create table "([^"]*)"\."([^"]*)"`)
var createIndexRegexp = regexp.MustCompile(`(?m)\s*create index (\w+).*\n\s*on "([^"]*)"\."([^"]*)"`)
var addExcludeConstraintRegexp = regexp.MustCompile(`(?m)\s*alter table "([^"]*)"\."([^"]*)".*\n\s*add constraint (\w+) exclude using (\w+).*`)

func createIndexesE(cmd *cobra.Command, args []string) error {
	var entityName string
	if len(args) == 1 {
		entityName = args[0]
	}

	skipConstraint := sflags.MustGetBool(cmd, "skip-constraint")
	drop := sflags.MustGetBool(cmd, "drop")

	cnt, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}

	var out []statement
	for _, el := range strings.Split(string(cnt), ";") {
		if m := createTableRegexp.FindStringSubmatch(el); m != nil {
			out = append(out, statement{ddlType: createTableDDL, tableName: m[2], schemaName: m[1], ddl: el})
			continue
		}
		if m := createIndexRegexp.FindStringSubmatch(el); m != nil {
			out = append(out, statement{ddlType: createIndexDDL, tableName: m[3], schemaName: m[2], objectName: m[1], ddl: el})
			continue
		}
		if m := addExcludeConstraintRegexp.FindStringSubmatch(el); m != nil {
			out = append(out, statement{ddlType: addExcludeConstraintDDL, tableName: m[2], schemaName: m[1], objectName: m[3], ddl: el})
			continue
		}
	}

	for _, o := range out {
		if o.ddlType == createTableDDL {
			continue
		}
		if skipConstraint && o.ddlType == addExcludeConstraintDDL {
			continue
		}
		if entityName != "" && o.tableName != entityName {
			continue
		}
		fmt.Println("-------------------------------------------")

		if drop {
			switch o.ddlType {
			case createIndexDDL:
				fmt.Printf("drop index if exists %q.%q", o.schemaName, o.objectName)
			case addExcludeConstraintDDL:
				fmt.Printf("alter table %q.%q drop constraint %s", o.schemaName, o.tableName, o.objectName)
			}
		} else {
			ddl := strings.TrimSpace(o.ddl)
			ddl = strings.Replace(ddl, "create index", "create index if not exists", 1)
			fmt.Print(ddl)
		}
		fmt.Println(";")
	}

	return nil
}

type ddlType int

const (
	createTableDDL ddlType = iota
	createIndexDDL
	addExcludeConstraintDDL
)

type statement struct {
	ddlType    ddlType
	tableName  string
	schemaName string
	objectName string
	ddl        string
}
