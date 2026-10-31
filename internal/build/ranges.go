package build

import (
	"context"
	"fmt"
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Only rewrite loops whose element types are actual Kafka driver records.
// A customer's unrelated Records() method must retain its original semantics.
func kafkaRanges(ctx context.Context, flags []string, listed []Package) (map[string]map[int]bool, error) {
	targets := []string{}
	seen := map[string]bool{}
	tests := false
	hasKafka := false
	for _, pkg := range listed {
		if pkg.ImportPath == "github.com/twmb/franz-go/pkg/kgo" || pkg.ImportPath == "github.com/segmentio/kafka-go" {
			hasKafka = true
		}
		if pkg.ForTest != "" {
			tests = true
			continue
		}
		if pkg.Module != nil && pkg.Module.Main && pkg.Module.Path != sdkPath && !strings.HasSuffix(pkg.ImportPath, ".test") && !seen[pkg.ImportPath] {
			targets = append(targets, pkg.ImportPath)
			seen[pkg.ImportPath] = true
		}
	}
	out := map[string]map[int]bool{}
	if !hasKafka || len(targets) == 0 {
		return out, nil
	}
	loaded, err := packages.Load(&packages.Config{Context: ctx, Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo, BuildFlags: flags, Tests: tests}, targets...)
	if err != nil {
		return nil, err
	}
	for _, pkg := range loaded {
		if len(pkg.Errors) > 0 {
			return nil, fmt.Errorf("type-check %s for Kafka correlation: %s", pkg.PkgPath, pkg.Errors[0])
		}
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(node ast.Node) bool {
				loop, ok := node.(*ast.RangeStmt)
				if !ok {
					return true
				}
				typ := pkg.TypesInfo.TypeOf(loop.X)
				if typ == nil {
					return true
				}
				slice, ok := typ.Underlying().(*types.Slice)
				if !ok {
					return true
				}
				element := types.Unalias(slice.Elem())
				if ptr, ok := element.(*types.Pointer); ok {
					element = types.Unalias(ptr.Elem())
				}
				named, ok := element.(*types.Named)
				if !ok || named.Obj().Pkg() == nil {
					return true
				}
				path, name := named.Obj().Pkg().Path(), named.Obj().Name()
				if !((path == "github.com/twmb/franz-go/pkg/kgo" && name == "Record") || (path == "github.com/segmentio/kafka-go" && name == "Message")) {
					return true
				}
				pos := pkg.Fset.PositionFor(loop.Pos(), false)
				if out[pos.Filename] == nil {
					out[pos.Filename] = map[int]bool{}
				}
				out[pos.Filename][pos.Offset] = true
				return true
			})
		}
	}
	return out, nil
}
