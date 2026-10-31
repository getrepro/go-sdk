package build

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"strconv"
)

const instrumentationPath = "github.com/getrepro/go-sdk/instrumentation"

// Transform preserves function bodies and evaluation order. In particular it
// does not wrap original bodies in closures, which would change recover/defer.
func Transform(path, pkg string, source []byte) ([]byte, int, error) {
	return transform(path, pkg, source, nil)
}

func transform(path, pkg string, source []byte, kafkaLoops map[int]bool) ([]byte, int, error) {
	fs := token.NewFileSet()
	file, err := parser.ParseFile(fs, path, source, parser.ParseComments)
	if err != nil {
		return nil, 0, err
	}
	used := map[string]bool{}
	shadowedBuiltin := false
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok {
			used[id.Name] = true
			if (id.Name == "recover" || id.Name == "panic") && id.Obj != nil {
				shadowedBuiltin = true
			}
		}
		return true
	})
	prefix := "__repro"
	for i := 0; used[prefix]; i++ {
		prefix = fmt.Sprintf("__repro%d", i)
	}
	// All generated names use a prefix absent from every original identifier.
	for {
		conflict := false
		for name := range used {
			if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
				conflict = true
				break
			}
		}
		if !conflict {
			break
		}
		prefix += "x"
	}
	count := 0
	if len(kafkaLoops) > 0 {
		ast.Inspect(file, func(node ast.Node) bool {
			loop, ok := node.(*ast.RangeStmt)
			if ok && kafkaLoops[fs.PositionFor(loop.Pos(), false).Offset] {
				loop.X = &ast.CallExpr{Fun: &ast.SelectorExpr{X: ast.NewIdent(prefix), Sel: ast.NewIdent("KafkaRecords")}, Args: []ast.Expr{loop.X}}
			}
			return true
		})
	}
	var instrument func(*ast.FuncType, *ast.BlockStmt, string)
	instrument = func(typ *ast.FuncType, body *ast.BlockStmt, name string) {
		if body == nil {
			return
		}
		count++
		index := count
		line := fs.Position(body.Pos()).Line
		ast.Inspect(body, func(node ast.Node) bool {
			if lit, ok := node.(*ast.FuncLit); ok {
				instrument(lit.Type, lit.Body, fmt.Sprintf("%s.func%d", name, count+1))
				return false
			}
			return true
		})
		args := nameFields(typ.Params, prefix+"arg", index)
		results := nameFields(typ.Results, prefix+"result", index)
		call := fmt.Sprintf("%scall%d", prefix, index)
		entry := fmt.Sprintf("%s := %s.Enter(%s,%s,%d,[]interface{}{%s})", call, prefix, strconv.Quote(pkg+"."+name), strconv.Quote(path), line, joinNames(args))
		// A direct defer preserves outer application recover handlers and observes
		// return values after the application's own deferred assignments.
		exit := fmt.Sprintf("defer func(){ %sp := recover(); %s.Exit(%s,[]interface{}{%s},%sp); if %sp != nil { panic(%sp) } }()", prefix, prefix, call, joinNames(results), prefix, prefix, prefix)
		if shadowedBuiltin {
			// Builtins can be shadowed. Referencing either in generated code would
			// change name resolution; leave unwinding entirely to the application.
			exit = fmt.Sprintf("defer func(){%s.Exit(%s,[]interface{}{%s},nil)}()", prefix, call, joinNames(results))
		}
		injected, parseErr := parser.ParseFile(token.NewFileSet(), "generated.go", "package p;func f(){"+entry+";"+exit+"}", 0)
		if parseErr != nil {
			panic(parseErr)
		}
		stmts := injected.Decls[0].(*ast.FuncDecl).Body.List
		body.List = append(stmts, body.List...)
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		name := fn.Name.Name
		if fn.Recv != nil {
			var b bytes.Buffer
			_ = format.Node(&b, fs, fn.Recv.List[0].Type)
			name = "(" + b.String() + ")." + name
		}
		instrument(fn.Type, fn.Body, name)
	}
	if count == 0 {
		return source, 0, nil
	}
	imp := &ast.ImportSpec{Name: ast.NewIdent(prefix), Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(instrumentationPath)}}
	file.Decls = append([]ast.Decl{&ast.GenDecl{Tok: token.IMPORT, Specs: []ast.Spec{imp}}}, file.Decls...)
	registrationName := pkg
	if file.Name.Name == "main" {
		registrationName = "main"
	}
	registration, _ := parser.ParseFile(token.NewFileSet(), "generated.go", "package p;func init(){"+prefix+".RegisterPackage("+strconv.Quote(registrationName)+")}", 0)
	file.Decls = append(file.Decls, registration.Decls[0])
	var out bytes.Buffer
	printerConfig := printer.Config{Mode: printer.SourcePos | printer.UseSpaces | printer.TabIndent, Tabwidth: 8}
	if err := printerConfig.Fprint(&out, fs, file); err != nil {
		return nil, 0, err
	}
	return out.Bytes(), count, nil
}

func nameFields(fields *ast.FieldList, prefix string, index int) []string {
	var names []string
	if fields == nil {
		return names
	}
	for _, field := range fields.List {
		if len(field.Names) == 0 {
			field.Names = []*ast.Ident{ast.NewIdent(fmt.Sprintf("%s_%d_%d", prefix, index, len(names)))}
		}
		for _, id := range field.Names {
			if id.Name == "_" {
				id.Name = fmt.Sprintf("%s_%d_%d", prefix, index, len(names))
			}
			names = append(names, id.Name)
		}
	}
	return names
}

func joinNames(names []string) string {
	var b bytes.Buffer
	for i, name := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(name)
	}
	return b.String()
}
