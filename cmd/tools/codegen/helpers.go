package codegen

import (
	"go/parser"
	"go/token"
	"io"
	"log"
	"os"
	"path"
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
	filename := path.Join(outPath, outFileName+"_gen.go")
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
		//nolint:revive // calls to log.Fatal only in main() or init() functions (revive)
		log.Fatal(err)
	}
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
	// Split the string by underscores
	words := strings.Split(s, "_")

	// Capitalize the first letter of each word
	for i, word := range words {
		// Convert first rune to upper and the rest to lower case
		words[i] = cases.Title(language.AmericanEnglish).String(strings.ToLower(word))
	}

	// Join them back into a single string
	return strings.Join(words, "")
}

func IsASCIIUpper(c rune) bool {
	return 'A' <= c && c <= 'Z'
}
