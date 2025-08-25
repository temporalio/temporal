package codegen

import (
	"go/parser"
	"go/token"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func GenerateToFile[T any](
	generator func(io.Writer, T) error,
	generatorArg T,
	outPath string,
	outFileName string,
) {
	filename := filepath.Join(outPath, outFileName+"_gen.go")
	w, err := os.Create(filename)
	FatalIfErr(err)
	defer func() {
		FatalIfErr(w.Close())
		_, err = parser.ParseFile(token.NewFileSet(), filename, nil, parser.SkipObjectResolution)
		FatalIfErr(err)
	}()
	err = generator(w, generatorArg)
	FatalIfErr(err)
}

func GenerateTemplateToWriter(tmpl string, data any, w io.Writer) error {
	t, err := template.New("code").Parse(tmpl)
	if err != nil {
		return err
	}
	return t.Execute(w, data)
}

func GenerateTemplateToFile(
	tmpl string,
	data any,
	outPath string,
	outFileName string,
) {
	GenerateToFile(func(w io.Writer, data any) error {
		return GenerateTemplateToWriter(tmpl, data, w)
	}, data, outPath, outFileName)
}

func FatalIfErr(err error) {
	if err != nil {
		//nolint:revive // okay to call Fatal here since this is part of the build process, not the server.
		log.Fatal(err)
	}
}

func Fatalf(format string, v ...any) {
	//nolint:revive // okay to call Fatal here since this is part of the build process, not the server.
	log.Fatalf(format, v...)
}

func CamelCaseToSnakeCase(s string) string {
	if s == "" {
		return ""
	}
	t := make([]rune, 0, len(s)+5)
	for i, c := range s {
		if IsASCIIUpper(c) {
			if i != 0 {
				t = append(t, '_')
			}
			c ^= ' ' // Make it a lower letter.
		}
		t = append(t, c)
	}
	return string(t)
}

func SnakeCaseToPascalCase(s string) string {
	var b strings.Builder
	// Capitalize the first letter of each word split by underscore
	for word := range strings.SplitSeq(s, "_") {
		// Convert first rune to upper and the rest to lower case
		b.WriteString(cases.Title(language.AmericanEnglish).String(strings.ToLower(word)))
	}
	// Join them back into a single string
	return b.String()
}

func IsASCIIUpper(c rune) bool {
	return 'A' <= c && c <= 'Z'
}
